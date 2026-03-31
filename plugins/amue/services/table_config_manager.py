"""
Gestionnaire de configuration des tables AMUE en base de données.

================================================================================
RÔLE DU MODULE
================================================================================

Remplace la variable Airflow 'amue_tables_to_import' par une table PostgreSQL
dédiée : splus_admin.amue_tables

Colonnes gérées :
    - table_name      : Nom de la table (PK)
    - enabled         : Table active ou non
    - primary_key     : Clés primaires pour UPSERT (CSV)
    - delta           : Colonne de date pour import différentiel
    - fingerprint_api : Hash structure originale API + PKs API
    - fingerprint_ul  : Hash structure transformée PG + PKs config
    - setup_status    : État du setup (pending / ready / blocked)
    - updated_at      : Timestamp de dernière modification

================================================================================
"""
import logging
from typing import Dict, List, Optional

from psycopg2.extras import execute_values

from common.utils.database.hooks import create_postgres_hook

logger = logging.getLogger(__name__)

_TABLE = "splus_admin.amue_tables"


class TableConfigManager:
    """
    Accès à la configuration des tables stockée dans splus_admin.amue_tables.

    Format de retour :
        {
            'table_name':      str,
            'enable':          bool,  # enabled
            'primary_key':     str,   # primary_key
            'delta':           str,   # delta
            'fingerprint_API': str,   # fingerprint_api
            'fingerprint_UL':  str,   # fingerprint_ul
            'setup_status':    str,   # setup_status (pending/ready/blocked)
        }

    Example:
        >>> mgr = TableConfigManager()
        >>> tables = mgr.get_tables_config()
        >>> mgr.save_primary_keys('CSKS', 'BUKRS,KOSTL')
    """

    def __init__(self, postgres_hook=None):
        self._hook = postgres_hook or create_postgres_hook(schema='public')

    # =========================================================================
    # LECTURE
    # =========================================================================

    def get_tables_config(self) -> List[Dict]:
        """
        Retourne toutes les tables — format dict rétro-compatible avec l'ancien JSON.

        Returns:
            Liste de dicts avec les clés : name, enable, primary_key, delta,
            fingerprint_API, fingerprint_UL

        Raises:
            Exception: Si la requête SQL échoue (pour déclencher le retry du caller)
        """
        try:
            rows = self._hook.get_records(
                f"SELECT table_name, enabled, primary_key, delta, "
                f"fingerprint_api, fingerprint_ul, setup_status "
                f"FROM {_TABLE} ORDER BY table_name"
            )
            result = [self._row_to_dict(row) for row in (rows or [])]
            logger.info(f"[TABLE_CONFIG] {len(result)} tables chargées depuis la BDD")
            return result
        except Exception as e:
            logger.error(f"[TABLE_CONFIG] Erreur chargement config tables: {e}")
            raise

    def get_table_metadata(self, table_name: str) -> Optional[Dict]:
        """
        Config d'une seule table ou None si non trouvée.

        Args:
            table_name: Nom de la table (insensible à la casse)

        Returns:
            Dict avec les clés de get_tables_config(), ou None
        """
        try:
            row = self._hook.get_first(
                f"SELECT table_name, enabled, primary_key, delta, "
                f"fingerprint_api, fingerprint_ul, setup_status "
                f"FROM {_TABLE} WHERE table_name = %s",
                parameters=(table_name.upper(),)
            )
            if not row:
                logger.warning(f"[TABLE_CONFIG] Table {table_name} non trouvée en BDD")
                return None
            return self._row_to_dict(row)
        except Exception as e:
            logger.warning(f"[TABLE_CONFIG] Erreur lecture {table_name}: {e}")
            return None

    # =========================================================================
    # ÉCRITURE
    # =========================================================================

    def save_tables_config(self, tables: List[Dict]) -> None:
        """
        UPDATE batch des métadonnées (fingerprints, primary_key) en une seule requête.

        Args:
            tables: Liste de dicts au format get_tables_config()

        Raises:
            Exception: Si l'UPDATE échoue (pour déclencher le retry du caller)
        """
        rows = [
            (
                table.get('fingerprint_API', ''),
                table.get('fingerprint_UL', ''),
                table.get('primary_key', ''),
                table.get('table_name', '').upper(),
            )
            for table in tables
            if table.get('table_name', '').strip()
        ]
        if not rows:
            logger.info("[TABLE_CONFIG] Aucune table à sauvegarder")
            return
        try:
            conn = self._hook.get_conn()
            cursor = conn.cursor()
            try:
                execute_values(
                    cursor,
                    f"""UPDATE {_TABLE} AS t
                        SET fingerprint_api = v.fp_api,
                            fingerprint_ul  = v.fp_ul,
                            primary_key     = v.pk,
                            updated_at      = NOW()
                        FROM (VALUES %s) AS v(fp_api, fp_ul, pk, tname)
                        WHERE t.table_name = v.tname""",
                    rows,
                    template="(%s, %s, %s, %s)",
                )
                conn.commit()
            finally:
                cursor.close()
            logger.info(f"[TABLE_CONFIG] {len(rows)} table(s) sauvegardée(s) (batch)")
        except Exception as e:
            logger.error(f"[TABLE_CONFIG] Erreur sauvegarde batch: {e}")
            raise

    def save_primary_keys(self, table_name: str, primary_keys: str) -> None:
        """
        UPDATE primary_key pour une seule table.

        Args:
            table_name: Nom de la table
            primary_keys: Clés primaires séparées par virgules
        """
        try:
            self._hook.run(
                f"UPDATE {_TABLE} SET primary_key = %s, updated_at = NOW() "
                f"WHERE table_name = %s",
                parameters=(primary_keys, table_name.upper())
            )
            logger.info(f"[TABLE_CONFIG] PKs sauvegardées pour {table_name}: {primary_keys}")
        except Exception as e:
            logger.error(f"[TABLE_CONFIG] Erreur sauvegarde PKs {table_name}: {e}")

    def reset_table_metadata(self, table_name: str) -> bool:
        """
        Vide fingerprint_api et fingerprint_ul pour une table.

        Utile en cas de changement de structure ou de réimport complet.

        Args:
            table_name: Nom de la table à réinitialiser

        Returns:
            True si succès, False en cas d'erreur
        """
        try:
            self._hook.run(
                f"""UPDATE {_TABLE}
                    SET fingerprint_api = '',
                        fingerprint_ul  = '',
                        updated_at      = NOW()
                    WHERE table_name = %s""",
                parameters=(table_name.upper(),)
            )
            logger.info(f"[TABLE_CONFIG] Métadonnées réinitialisées pour {table_name}")
            return True
        except Exception as e:
            logger.error(f"[TABLE_CONFIG] Erreur réinitialisation {table_name}: {e}")
            return False

    # =========================================================================
    # HELPERS PRIVÉS
    # =========================================================================

    # =========================================================================
    # SETUP (gestion du setup_status et des fingerprints par la DAG setup)
    # =========================================================================

    def save_setup_result(self, table_name: str, fingerprint_api: str,
                          fingerprint_ul: str, primary_keys: str) -> None:
        """
        Sauvegarde atomique du résultat du setup pour une table.

        Met à jour fingerprints, primary_key et passe setup_status à 'ready'.

        Args:
            table_name: Nom de la table
            fingerprint_api: Hash structure API
            fingerprint_ul: Hash structure PG
            primary_keys: Clés primaires CSV
        """
        try:
            self._hook.run(
                f"""UPDATE {_TABLE}
                    SET fingerprint_api = %s,
                        fingerprint_ul  = %s,
                        primary_key     = %s,
                        setup_status    = 'ready',
                        updated_at      = NOW()
                    WHERE table_name = %s""",
                parameters=(
                    fingerprint_api,
                    fingerprint_ul,
                    primary_keys,
                    table_name.upper(),
                )
            )
            logger.info(f"[TABLE_CONFIG] Setup result sauvegardé pour {table_name} (status=ready)")
        except Exception as e:
            logger.error(f"[TABLE_CONFIG] Erreur sauvegarde setup result {table_name}: {e}")
            raise

    def set_setup_status(self, table_name: str, status: str) -> None:
        """
        Met à jour uniquement le setup_status d'une table.

        Args:
            table_name: Nom de la table
            status: Nouveau statut ('pending', 'ready', 'blocked')
        """
        try:
            self._hook.run(
                f"UPDATE {_TABLE} SET setup_status = %s, updated_at = NOW() "
                f"WHERE table_name = %s",
                parameters=(status, table_name.upper())
            )
            logger.info(f"[TABLE_CONFIG] setup_status={status} pour {table_name}")
        except Exception as e:
            logger.error(f"[TABLE_CONFIG] Erreur set_setup_status {table_name}: {e}")

    @staticmethod
    def _row_to_dict(row) -> Dict:
        """Convertit une ligne SQL en dict."""
        return {
            'table_name':      row[0],
            'enable':          row[1],
            'primary_key':     row[2] or '',
            'delta':           row[3] or '',
            'fingerprint_API': row[4] or '',
            'fingerprint_UL':  row[5] or '',
            'setup_status':    row[6] if len(row) > 6 else 'pending',
        }
