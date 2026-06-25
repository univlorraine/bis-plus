# ecc/tasks/import_dag/sync_to_active.py
"""Task de synchronisation ECC : copie les données du schéma inactif vers l'actif."""
import logging
from typing import Dict, List

from airflow.sdk import task

from common.application.bluegreen.bluegreen_manager import BlueGreenManager
from common.infrastructure.database.hooks import create_postgres_hook
from ecc.infrastructure.config.settings import ECCDefaults

logger = logging.getLogger(__name__)


@task(task_id='sync_ecc_to_active', multiple_outputs=False)
def sync_to_active(imported: List[Dict]) -> Dict:
    """
    Synchronise les données ECC du schéma inactif vers le schéma actif.

    Ne s'exécute que si tous les imports ont réussi (dépendance Airflow).
    Si cette tâche échoue, l'actif n'est pas modifié (pas de données partielles).
    Utilise une seule transaction PostgreSQL pour tous les UPSERT.

    Args:
        imported: Résultats des tâches import_ecc_data

    Returns:
        {'tables_synced': int, 'rows_synced': int}
    """
    manager = BlueGreenManager()
    active = manager.get_active_schema()

    # Récupérer le schéma source (inactif) depuis les résultats d'import
    inactive = next(
        (r['target_schema'] for r in imported if r and r.get('target_schema') and r['target_schema'] != active),
        None
    )

    if not inactive or inactive == active:
        logger.info("[ECC] Sync ignorée : pas de schéma inactif distinct (premier lancement ?)")
        return {'tables_synced': 0, 'rows_synced': 0}

    # Tables importées avec succès
    table_names = list({r['table_name'] for r in imported if r and r.get('status') == 'success'})
    if not table_names:
        logger.warning("[ECC] Aucune table à synchroniser")
        return {'tables_synced': 0, 'rows_synced': 0}

    pg_hook = create_postgres_hook(schema='splus_admin')

    # PKs depuis splus_admin.amue_tables
    pk_rows = pg_hook.get_records(
        """
        SELECT table_name, primary_key FROM splus_admin.amue_tables
        WHERE table_name = ANY(%s) AND ecc_query IS NOT NULL AND ecc_query != ''
        """,
        parameters=(table_names,)
    )
    pk_map = {name: [pk.strip().lower() for pk in pks.split(',') if pk.strip()] for name, pks in pk_rows}

    conn = create_postgres_hook().get_conn()
    conn.autocommit = False
    total_rows = 0
    tables_synced = 0

    try:
        cursor = conn.cursor()

        for table_name in table_names:
            pks = pk_map.get(table_name)
            if not pks:
                logger.warning(f"[ECC] Sync: pas de PK pour {table_name}, ignoré")
                continue

            # Colonnes depuis information_schema (inactif)
            cursor.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """, [inactive, table_name.lower()])
            cols = [row[0] for row in cursor.fetchall()]
            if not cols:
                logger.warning(f"[ECC] Sync: table {inactive}.{table_name} introuvable, ignorée")
                continue

            col_list   = ', '.join(f'"{c}"' for c in cols)
            set_clause = ', '.join(f'"{c}" = EXCLUDED."{c}"' for c in cols if c not in pks)
            conflict   = ', '.join(f'"{pk}"' for pk in pks)

            sql = f"""
                INSERT INTO {active}.{table_name} ({col_list})
                SELECT {col_list} FROM {inactive}.{table_name}
                WHERE _source = %s
                ON CONFLICT ({conflict}) DO UPDATE
                    SET {set_clause}
                WHERE {active}.{table_name}._source != %s
            """
            cursor.execute(sql, [ECCDefaults.SOURCE_NAME, ECCDefaults.PROTECTED_SOURCE])
            synced = cursor.rowcount
            total_rows += synced
            tables_synced += 1
            logger.info(f"[ECC] Sync {table_name}: {synced} ligne(s) → {active}")

        conn.commit()
        logger.info(f"[ECC] Sync terminée : {tables_synced} table(s), {total_rows} ligne(s)")
        return {'tables_synced': tables_synced, 'rows_synced': total_rows}

    except Exception:
        conn.rollback()   # active inchangé si erreur
        raise
    finally:
        conn.close()
