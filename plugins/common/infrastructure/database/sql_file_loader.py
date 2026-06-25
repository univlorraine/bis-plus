"""Layer: infrastructure

Lecture de fichiers .sql depuis le disque, avec vérification d'intégrité
optionnelle par manifest SHA-256.

Utilisée par `common.application.bluegreen.view_switcher` (vues custom) pour
éviter que chaque consommateur réimplémente sa propre lecture de fichier `.sql`.
"""
import hashlib
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def verify_sql_file_integrity(sql_file: Path) -> None:
    """Vérifie l'intégrité d'un fichier SQL via un manifest SHA-256.

    Si le manifest `.manifest.sha256` existe dans le répertoire du fichier, le
    hash du fichier doit y correspondre. Lève ValueError si compromis, log un
    avertissement si le manifest est absent (permet l'usage sans manifest en dev).
    """
    manifest_path = sql_file.parent / ".manifest.sha256"
    if not manifest_path.exists():
        logger.debug(f"[SQL_FILE] Pas de manifest d'intégrité pour {sql_file.name}")
        return
    expected: dict = {}
    for line in manifest_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith('#'):
            parts = line.split(None, 1)
            if len(parts) == 2:
                expected[parts[1].strip()] = parts[0]
    if sql_file.name not in expected:
        logger.debug(f"[SQL_FILE] {sql_file.name} absent du manifest — exécution permise")
        return
    actual = hashlib.sha256(sql_file.read_bytes()).hexdigest()
    if actual != expected[sql_file.name]:
        raise ValueError(
            f"[SQL_FILE] Intégrité fichier SQL compromise : {sql_file.name} "
            f"(attendu {expected[sql_file.name][:16]}…, obtenu {actual[:16]}…)"
        )


def read_sql_file(sql_file: Path, *, verify_integrity: bool = True) -> str:
    """Lit le contenu d'un fichier .sql, en vérifiant son intégrité par défaut."""
    if verify_integrity:
        verify_sql_file_integrity(sql_file)
    return sql_file.read_text(encoding="utf-8")
