"""
Configuration FabAuthManager — Authentification CAS classique (protocole CAS v2/v3).

Flux d'authentification :
  1. Utilisateur non authentifié → redirigé vers CAS_SERVER_URL/login?service=<callback>
  2. Authentification sur la page CAS de l'université
  3. CAS redirige vers /login?ticket=ST-xxx
  4. Airflow valide le ticket auprès du CAS (serviceValidate)
  5. Utilisateur trouvé ou créé dans la base FAB, session ouverte

Variables d'environnement (.env) :
  CAS_SERVER_URL   — URL racine du CAS (ex: https://cas.votre-universite.fr)  [obligatoire]
  CAS_VERSION      — Version du protocole CAS : 2 ou 3 (défaut: 2)             [optionnel]
  CAS_DEFAULT_ROLE — Rôle Airflow attribué à la première connexion              [optionnel, défaut: Viewer]
  CAS_SERVICE_URL  — URL de callback si auto-détection incorrecte               [optionnel]
  CAS_ALLOWED_USERS — Usernames autorisés à se connecter (vide = tous)          [optionnel, déprécié si pré-inscription active]
  CAS_ADMIN_USERS  — Usernames qui peuvent avoir le rôle Admin (vide = aucune   [optionnel]
                     restriction). Seuls ces utilisateurs peuvent être Admin ;
                     tout autre utilisateur est systématiquement ramené au rôle
                     CAS_DEFAULT_ROLE, même si un admin le promet via l'UI.

Protection Admin (double couche) :
  Couche A — au login   : le rôle est synchronisé avec CAS_ADMIN_USERS à chaque connexion.
  Couche B — SecurityManager : update_user() bloque toute tentative de promotion Admin
                               pour un utilisateur absent de CAS_ADMIN_USERS.

Dégradations gracieuses :
  - CAS_SERVER_URL absent/placeholder → fallback AUTH_DB + avertissement
  - Module python-cas manquant        → fallback AUTH_DB + message rebuild image
"""
import logging
import os

logger = logging.getLogger(__name__)

_CAS_SERVER_URL   = os.environ.get("CAS_SERVER_URL", "").rstrip("/")
_CAS_VERSION      = int(os.environ.get("CAS_VERSION", "2"))
_CAS_DEFAULT_ROLE = os.environ.get("CAS_DEFAULT_ROLE", "Viewer")
_CAS_SERVICE_URL  = os.environ.get("CAS_SERVICE_URL", "")

# Whitelist de connexion (vide = tous les utilisateurs CAS sont acceptés)
_CAS_ALLOWED_USERS = {
    u.strip() for u in os.environ.get("CAS_ALLOWED_USERS", "").split(",") if u.strip()
}

# Whitelist Admin — seuls ces usernames peuvent avoir le rôle Admin.
# Vide = aucune restriction sur le rôle Admin (comportement héritage).
_CAS_ADMIN_USERS = {
    u.strip() for u in os.environ.get("CAS_ADMIN_USERS", "").split(",") if u.strip()
}

_PLACEHOLDERS = ("<À_COMPLÉTER>", "REMPLACER_PAR")

def _is_set(val: str) -> bool:
    return bool(val) and not any(val.startswith(p) for p in _PLACEHOLDERS)


# ══════════════════════════════════════════════════════════════════════════════
# Sélection du mode d'authentification
# ══════════════════════════════════════════════════════════════════════════════

def _warn(lines: str, level: str = "warning") -> None:
    log = getattr(logger, level)
    for line in lines.splitlines():
        log(line)


if not _is_set(_CAS_SERVER_URL):
    # ── CAS non configuré → AUTH_DB ───────────────────────────────────────────
    from flask_appbuilder.security.manager import AUTH_DB

    AUTH_TYPE = AUTH_DB

    _warn("""
╔══════════════════════════════════════════════════════════════════════════════╗
║                                                                              ║
║   ⚠️  AVERTISSEMENT SÉCURITÉ — AUTHENTIFICATION LOCALE ACTIVE  ⚠️              ║
║                                                                              ║
║   CAS_SERVER_URL n'est pas défini dans .env.                                 ║
║   Airflow utilise l'authentification locale (AUTH_DB).                       ║
║   RÉSERVÉ AU DÉVELOPPEMENT — ne jamais utiliser en production.               ║
║                                                                              ║
║   Pour activer le CAS, ajoutez dans .env :                                   ║
║     CAS_SERVER_URL=https://cas.votre-universite.fr                           ║
║   puis : ./manage.sh restart                                                 ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
""")

else:
    # ── CAS_SERVER_URL présent — tentative d'import python-cas ────────────────
    try:
        from cas import CASClient
        _cas_available = True
    except ImportError:
        _cas_available = False

    if not _cas_available:
        # ── python-cas manquant → AUTH_DB + message rebuild ───────────────────
        from flask_appbuilder.security.manager import AUTH_DB

        AUTH_TYPE = AUTH_DB

        _warn("""
╔══════════════════════════════════════════════════════════════════════════════╗
║   ✗  ERREUR — MODULE python-cas MANQUANT                                     ║
║                                                                              ║
║   CAS_SERVER_URL est défini mais le module 'python-cas' n'est pas installé   ║
║   dans l'image Docker. L'image doit être reconstruite.                       ║
║                                                                              ║
║   Commandes :                                                                ║
║     ./manage.sh build    ← reconstruit l'image avec python-cas               ║
║     ./manage.sh restart  ← relance les containers                            ║
║                                                                              ║
║   Fallback AUTH_DB actif — accès local uniquement.                           ║
╚══════════════════════════════════════════════════════════════════════════════╝
""", level="error")

    else:
        # ── CAS classique (protocole ticket ST) ───────────────────────────────
        from flask import flash, redirect, request, url_for
        from flask_appbuilder import expose
        from flask_appbuilder.security.views import AuthDBView
        from flask_login import login_user

        from airflow.providers.fab.auth_manager.security_manager.override import (
            FabAirflowSecurityManagerOverride,
        )

        def _cas_client(service_url: str) -> CASClient:
            return CASClient(
                version=_CAS_VERSION,
                service_url=service_url,
                server_url=_CAS_SERVER_URL + "/",
            )

        def _resolve_target_role(sm, username: str):
            """Retourne le rôle FAB attendu selon CAS_ADMIN_USERS."""
            role_name = "Admin" if (_CAS_ADMIN_USERS and username in _CAS_ADMIN_USERS) else _CAS_DEFAULT_ROLE
            return sm.find_role(role_name), role_name

        def _sync_role(sm, user, username: str) -> None:
            """
            Couche A — synchronise le rôle de l'utilisateur avec la config .env.

            Appelée à chaque connexion CAS réussie. Si CAS_ADMIN_USERS est vide,
            aucune synchronisation n'est effectuée (comportement héritage).
            """
            if not _CAS_ADMIN_USERS:
                return
            target_role, target_name = _resolve_target_role(sm, username)
            if target_role is None:
                logger.error("[CAS] Rôle '%s' introuvable dans FAB", target_name)
                return
            current_names = {r.name for r in (user.roles or [])}
            if current_names != {target_name}:
                user.roles = [target_role]
                sm.update_user(user)
                logger.info(
                    "[CAS] Rôle de '%s' synchronisé : %s → %s",
                    username, current_names or "∅", target_name,
                )

        class CASAuthView(AuthDBView):
            """
            Remplace la vue de login FAB pour implémenter le protocole CAS.

            - Sans ticket  → redirige vers la page de connexion CAS
            - Avec ticket  → valide auprès du CAS, crée/trouve l'utilisateur, ouvre la session
            """

            @expose("/login/")
            @expose("/login/<string:pk>")
            def login(self, pk=None):
                service_url = _CAS_SERVICE_URL or url_for("CASAuthView.login", _external=True)
                ticket = request.args.get("ticket")

                if not ticket:
                    return redirect(_cas_client(service_url).get_login_url())

                username, attributes, _pgtiou = _cas_client(service_url).verify_ticket(ticket)

                if not username:
                    logger.warning("[CAS] Ticket invalide ou expiré (ticket=%s…)", ticket[:12])
                    flash("Authentification CAS échouée — ticket invalide ou expiré.", "danger")
                    return redirect(url_for("CASAuthView.login"))

                if _CAS_ALLOWED_USERS and username not in _CAS_ALLOWED_USERS:
                    logger.warning("[CAS] Accès refusé : '%s' absent de la whitelist", username)
                    flash(
                        f"Accès refusé : le compte « {username} » n'est pas autorisé "
                        "sur cette instance Airflow. Contactez l'administrateur.",
                        "danger",
                    )
                    return redirect(url_for("CASAuthView.login"))

                sm   = self.appbuilder.sm
                user = sm.find_user(username=username)

                cas_email      = attributes.get("mail") or attributes.get("email") or ""
                cas_first_name = attributes.get("givenname") or attributes.get("given_name") or ""
                cas_last_name  = attributes.get("sn") or attributes.get("family_name") or ""

                if not user:
                    logger.warning("[CAS] Accès refusé : '%s' non pré-inscrit dans Airflow", username)
                    flash(
                        f"Accès refusé : le compte « {username} » n'est pas pré-inscrit. "
                        "Contactez l'administrateur.",
                        "danger",
                    )
                    return redirect(url_for("CASAuthView.login"))
                else:
                    # Compléter le profil si l'utilisateur a été pré-créé avec des données placeholder
                    _placeholder = user.email.endswith("@cas.local") or not user.email
                    if _placeholder and (cas_email or cas_first_name or cas_last_name):
                        user.email      = cas_email      or user.email
                        user.first_name = cas_first_name or user.first_name
                        user.last_name  = cas_last_name  or user.last_name
                        sm.update_user(user)
                        logger.info("[CAS] Profil de %s complété depuis les attributs CAS", username)

                    # Couche A — utilisateur existant : synchronisation du rôle
                    _sync_role(sm, user, username)

                login_user(user, remember=False)
                logger.info("[CAS] Connexion réussie : %s", username)
                return redirect(self.appbuilder.get_url_for_index)

        # ── Couche B — SecurityManager : blocage des promotions Admin via l'UI ──
        # Intercepte update_user() pour empêcher l'attribution du rôle Admin
        # à tout utilisateur absent de CAS_ADMIN_USERS.
        # Ne s'active que si CAS_ADMIN_USERS est non vide.
        if _CAS_ADMIN_USERS:
            _orig_update_user = FabAirflowSecurityManagerOverride.update_user

            def _guarded_update_user(self, user):
                admin_role = self.find_role("Admin")
                if admin_role is not None and admin_role in (user.roles or []):
                    if user.username not in _CAS_ADMIN_USERS:
                        user.roles = [r for r in user.roles if r != admin_role]
                        logger.warning(
                            "[SEC] Promotion Admin bloquée pour '%s' — absent de CAS_ADMIN_USERS",
                            user.username,
                        )
                return _orig_update_user(self, user)

            FabAirflowSecurityManagerOverride.update_user = _guarded_update_user
            logger.info(
                "[SEC] Couche B active — rôle Admin réservé à : %s",
                ", ".join(sorted(_CAS_ADMIN_USERS)),
            )

        # FAB_SECURITY_MANAGER_CLASS est ignoré par Airflow 3.x.
        # Monkey-patch direct sur la classe utilisée par register_views() (ligne 186 + 454 de override.py).
        FabAirflowSecurityManagerOverride.authdbview = CASAuthView
        logger.info("[CAS] authdbview remplacé par CASAuthView sur FabAirflowSecurityManagerOverride")

        logger.info(
            "[CAS] Authentification CAS v%s configurée (serveur : %s)",
            _CAS_VERSION, _CAS_SERVER_URL,
        )
