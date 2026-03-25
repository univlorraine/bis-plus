# common/tasks/restore_inactive.py
"""Task de restauration du schéma inactif depuis l'actif (sur échec d'import)."""
import logging
from typing import Dict, List

from airflow.exceptions import AirflowSkipException
from airflow.sdk import task
from airflow.task.trigger_rule import TriggerRule

from amue.services.bluegreen.bluegreen_manager import BlueGreenManager
from amue.utils.database.hooks import create_postgres_hook

logger = logging.getLogger(__name__)


@task(task_id='restore_inactive', trigger_rule=TriggerRule.ALL_DONE)
def restore_inactive(tables: List[Dict], source_name: str, import_results: List) -> Dict:
    """
    Restaure les données <source_name> du schéma inactif depuis le schéma actif.

    S'exécute après la fin de TOUS les imports (ALL_DONE), mais ne restaure que
    si au moins un import a échoué (résultat None dans import_results).
    Pour chaque table : supprime les données partielles dans l'inactif, puis
    recopie depuis l'actif le dernier état cohérent connu.
    Les données des autres sources (_source != source_name) sont intouchées.
    Opération en transaction unique : rollback complet si erreur.

    Args:
        tables:         Liste de configs tables (output de select_tables / select_ecc_tables).
                        Chaque dict doit contenir 'table_name' et 'target_schema' (= inactif).
        source_name:    Source à restaurer. Ex : 'sifac_plus' pour AMUE, 'ecc' pour ECC.
        import_results: Résultats des tâches import (liste agrégée du expand).
                        Les tâches échouées produisent None dans cette liste.

    Returns:
        {'tables_restored': int, 'rows_restored': int}
    """
    # import_data n'a pas tourné (wait_api_sensor ou check_setup_status en échec)
    if import_results is None:
        raise AirflowSkipException("import_data n'a pas tourné — restauration ignorée")

    if not tables:
        raise AirflowSkipException("Aucune table configurée")

    # Tous les imports ont réussi : aucun None dans les résultats
    if all(r is not None for r in import_results):
        raise AirflowSkipException("Tous les imports ont réussi — restauration ignorée")

    inactive = tables[0]['target_schema']
    manager = BlueGreenManager()
    active = manager.get_active_schema()

    if not inactive or inactive == active:
        logger.warning(
            f"[RESTORE] Schéma inactif non distinct de l'actif ({inactive!r}), restauration ignorée"
        )
        return {'tables_restored': 0, 'rows_restored': 0}

    table_names = list({t['table_name'] for t in tables if t and t.get('table_name')})
    logger.info(
        f"[RESTORE] Restauration de {len(table_names)} table(s) "
        f"_source='{source_name}' : {inactive} ← {active}"
    )

    conn = create_postgres_hook().get_conn()
    conn.autocommit = False
    tables_restored = 0
    rows_restored = 0

    try:
        cursor = conn.cursor()

        for table_name in table_names:
            tname = table_name.lower()

            # Vérifier existence dans l'inactif
            cursor.execute(
                """
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = %s AND table_name = %s
                )
                """,
                [inactive, tname],
            )
            if not cursor.fetchone()[0]:
                logger.debug(f"[RESTORE] {inactive}.{tname} absent, ignoré")
                continue

            # Supprimer les données partielles de ce run
            cursor.execute(
                f'DELETE FROM {inactive}."{tname}" WHERE _source = %s',
                [source_name],
            )
            deleted = cursor.rowcount

            # Restaurer depuis l'actif (si la table existe dans l'actif)
            cursor.execute(
                """
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = %s AND table_name = %s
                )
                """,
                [active, tname],
            )
            if cursor.fetchone()[0]:
                cursor.execute(
                    f"""
                    INSERT INTO {inactive}."{tname}"
                    SELECT * FROM {active}."{tname}" WHERE _source = %s
                    """,
                    [source_name],
                )
                inserted = cursor.rowcount
            else:
                inserted = 0
                logger.debug(f"[RESTORE] {active}.{tname} absent, pas de données à restaurer")

            rows_restored += inserted
            tables_restored += 1
            logger.info(
                f"[RESTORE] {tname}: {deleted} ligne(s) supprimée(s), "
                f"{inserted} ligne(s) restaurée(s)"
            )

        conn.commit()
        logger.info(
            f"[RESTORE] Terminé : {tables_restored} table(s), "
            f"{rows_restored} ligne(s) restaurée(s) _source='{source_name}'"
        )
        return {'tables_restored': tables_restored, 'rows_restored': rows_restored}

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
