#!/usr/bin/env python3
"""
Client PostgreSQL en pur Python stdlib — aucun package externe requis.
Implémente le protocole wire PostgreSQL v3 (sans SSL).

Variables d'environnement attendues (injectées par _pg_exec dans manage.sh) :
  PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD

Flags supportés (compatibles psql) :
  -c <sql>   Requête SQL à exécuter
  -A         Mode non-aligné (séparateur |)
  -t         Tuples uniquement (pas d'entêtes)
  -At / -tA  Sortie brute : valeurs séparées par |, une ligne par résultat
"""

import argparse
import base64
import hashlib
import hmac
import os
import socket
import struct
import sys


# ─── Primitives réseau ────────────────────────────────────────────────────────

def _recv(sock, n):
    """Lit exactement n octets depuis le socket."""
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("Connexion PostgreSQL fermée inopinément")
        buf += chunk
    return buf


def _read_msg(sock):
    """Lit un message backend : (type:bytes, payload:bytes)."""
    mtype = _recv(sock, 1)
    length = struct.unpack("!I", _recv(sock, 4))[0] - 4
    payload = _recv(sock, length) if length > 0 else b""
    return mtype, payload


# ─── Authentification ─────────────────────────────────────────────────────────

def _md5_password(password, user, salt):
    """Calcule le hash MD5 attendu par PostgreSQL : md5(md5(pwd+user)+salt)."""
    inner = hashlib.md5(password.encode() + user.encode()).hexdigest()
    outer = hashlib.md5(inner.encode() + salt).hexdigest()
    return ("md5" + outer).encode() + b"\x00"


def _scram_sha256_auth(password, payload, sock):
    """Implémente l'échange SCRAM-SHA-256 (RFC 5802)."""
    if b"SCRAM-SHA-256" not in payload:
        raise RuntimeError(f"SCRAM-SHA-256 non proposé par le serveur (payload: {payload!r})")

    # Étape 1 : SASLInitialResponse
    client_nonce = base64.b64encode(os.urandom(24)).decode()
    client_first_bare = f"n=,r={client_nonce}"
    client_first_msg = f"n,,{client_first_bare}".encode()

    mechanism = b"SCRAM-SHA-256\x00"
    content = mechanism + struct.pack("!i", len(client_first_msg)) + client_first_msg
    sock.sendall(b"p" + struct.pack("!I", len(content) + 4) + content)

    # Étape 2 : AuthenticationSASLContinue (auth=11)
    mtype, rpayload = _read_msg(sock)
    if mtype != b"R":
        raise RuntimeError(f"Message inattendu pendant SCRAM : {mtype!r}")
    auth_type = struct.unpack("!I", rpayload[:4])[0]
    if auth_type != 11:
        raise RuntimeError(f"Type auth SCRAM inattendu (attendu 11, reçu {auth_type})")
    server_first = rpayload[4:].decode()

    # Parsing : r=...,s=...,i=...
    parts = {}
    for part in server_first.split(","):
        k, _, v = part.partition("=")
        parts[k] = v
    server_nonce = parts["r"]
    salt = base64.b64decode(parts["s"])
    iterations = int(parts["i"])

    if not server_nonce.startswith(client_nonce):
        raise RuntimeError("Nonce serveur invalide (SCRAM-SHA-256)")

    # Étape 3 : calcul de la preuve et envoi du client-final-message
    channel_binding = base64.b64encode(b"n,,").decode()
    client_final_without_proof = f"c={channel_binding},r={server_nonce}"

    salted_password = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    client_key      = hmac.new(salted_password, b"Client Key", hashlib.sha256).digest()
    stored_key      = hashlib.sha256(client_key).digest()
    auth_message    = f"{client_first_bare},{server_first},{client_final_without_proof}".encode()
    client_sig      = hmac.new(stored_key, auth_message, hashlib.sha256).digest()
    client_proof    = bytes(a ^ b for a, b in zip(client_key, client_sig))

    client_final = f"{client_final_without_proof},p={base64.b64encode(client_proof).decode()}".encode()
    sock.sendall(b"p" + struct.pack("!I", len(client_final) + 4) + client_final)

    # Étape 4 : AuthenticationSASLFinal (auth=12)
    mtype, rpayload = _read_msg(sock)
    if mtype != b"R":
        raise RuntimeError(f"Message inattendu pendant SCRAM final : {mtype!r}")
    auth_type = struct.unpack("!I", rpayload[:4])[0]
    if auth_type != 12:
        raise RuntimeError(f"Type auth SCRAM final inattendu (attendu 12, reçu {auth_type})")
    # La boucle principale recevra ensuite auth=0 (AuthenticationOk)


def _startup_message(user, database):
    params = (
        b"user\x00" + user.encode() + b"\x00"
        b"database\x00" + database.encode() + b"\x00\x00"
    )
    return struct.pack("!II", len(params) + 8, 196608) + params


# ─── Connexion ────────────────────────────────────────────────────────────────

def pg_connect(host, port, dbname, user, password):
    """Établit une connexion PostgreSQL et gère l'authentification."""
    sock = socket.create_connection((host, int(port)), timeout=15)
    sock.sendall(_startup_message(user, dbname))

    while True:
        mtype, payload = _read_msg(sock)
        if mtype == b"R":
            auth = struct.unpack("!I", payload[:4])[0]
            if auth == 0:
                pass  # AuthenticationOk — on attend ReadyForQuery
            elif auth == 3:
                pwd = password.encode() + b"\x00"
                sock.sendall(b"p" + struct.pack("!I", len(pwd) + 4) + pwd)
            elif auth == 5:
                salt = payload[4:8]
                pwd = _md5_password(password, user, salt)
                sock.sendall(b"p" + struct.pack("!I", len(pwd) + 4) + pwd)
            elif auth == 10:
                _scram_sha256_auth(password, payload, sock)
            else:
                raise RuntimeError(f"Méthode d'auth PostgreSQL non supportée : {auth}")
        elif mtype == b"E":
            raise RuntimeError(_parse_error(payload))
        elif mtype == b"Z":
            return sock  # ReadyForQuery — connexion prête
        # S=ParameterStatus, K=BackendKeyData : ignorés


# ─── Parsers de messages ──────────────────────────────────────────────────────

def _parse_error(payload):
    pos, msgs = 0, {}
    while pos < len(payload):
        ftype = payload[pos : pos + 1]
        if ftype == b"\x00":
            break
        pos += 1
        end = payload.index(b"\x00", pos)
        msgs[ftype] = payload[pos:end].decode("utf-8", errors="replace")
        pos = end + 1
    return msgs.get(b"M", repr(payload))


def _parse_row_description(payload):
    """Extrait les noms de colonnes depuis un message RowDescription (T)."""
    n, pos = struct.unpack("!H", payload[:2])[0], 2
    cols = []
    for _ in range(n):
        end = payload.index(b"\x00", pos)
        cols.append(payload[pos:end].decode())
        pos = end + 1 + 18  # 18 octets de métadonnées par colonne
    return cols


def _parse_data_row(payload):
    """Extrait les valeurs d'une ligne depuis un message DataRow (D)."""
    n, pos = struct.unpack("!H", payload[:2])[0], 2
    row = []
    for _ in range(n):
        length = struct.unpack("!i", payload[pos : pos + 4])[0]
        pos += 4
        if length == -1:
            row.append(None)
        else:
            row.append(payload[pos : pos + length].decode("utf-8", errors="replace"))
            pos += length
    return row


# ─── Exécution ────────────────────────────────────────────────────────────────

def pg_execute(sock, sql):
    """Envoie une requête simple et retourne (colonnes, lignes)."""
    q = sql.encode() + b"\x00"
    sock.sendall(b"Q" + struct.pack("!I", len(q) + 4) + q)

    cols, rows = [], []
    while True:
        mtype, payload = _read_msg(sock)
        if mtype == b"T":
            cols = _parse_row_description(payload)
        elif mtype == b"D":
            rows.append(_parse_data_row(payload))
        elif mtype == b"E":
            raise RuntimeError(_parse_error(payload))
        elif mtype == b"Z":
            return cols, rows
        # C=CommandComplete, I=EmptyQuery, N=Notice : ignorés


def pg_close(sock):
    try:
        sock.sendall(b"X" + struct.pack("!I", 4))
        sock.close()
    except OSError:
        pass


# ─── Formatage de la sortie ───────────────────────────────────────────────────

def print_table(cols, rows):
    if not cols:
        return
    widths = [len(c) for c in cols]
    for row in rows:
        for i, v in enumerate(row):
            widths[i] = max(widths[i], len(str(v) if v is not None else ""))
    fmt = " | ".join(f"{{:<{w}}}" for w in widths)
    sep = "-+-".join("-" * w for w in widths)
    print(" " + fmt.format(*cols))
    print("-" + sep + "-")
    for row in rows:
        print(" " + fmt.format(*[str(v) if v is not None else "" for v in row]))
    n = len(rows)
    print(f"\n({n} ligne{'s' if n > 1 else ''})")


def print_tuples(cols, rows):
    """Format -At : valeurs brutes séparées par |."""
    for row in rows:
        print("|".join("" if v is None else str(v) for v in row))


# ─── Point d'entrée ───────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("-c", dest="sql")
    args, remaining = parser.parse_known_args()

    unaligned = tuples_only = False
    for flag in remaining:
        if flag.startswith("-") and not flag.startswith("--"):
            if "A" in flag:
                unaligned = True
            if "t" in flag:
                tuples_only = True

    if not args.sql:
        print('Usage: pg_client.py -c "SQL"', file=sys.stderr)
        sys.exit(1)

    host     = os.environ.get("PG_HOST", "")
    port     = os.environ.get("PG_PORT", "5432")
    dbname   = os.environ.get("PG_DB", "")
    user     = os.environ.get("PG_USER", "")
    password = os.environ.get("PG_PASSWORD", "")

    try:
        sock = pg_connect(host, port, dbname, user, password)
        cols, rows = pg_execute(sock, args.sql)
        pg_close(sock)
    except (OSError, RuntimeError) as e:
        print(f"Erreur : {e}", file=sys.stderr)
        sys.exit(1)

    if tuples_only or unaligned:
        print_tuples(cols, rows)
    else:
        print_table(cols, rows)


if __name__ == "__main__":
    main()
