"""Task de validation agrégée des résultats de vérification."""
import logging
from typing import Dict, List

from airflow.sdk import task
from airflow.exceptions import AirflowException

logger = logging.getLogger(__name__)


@task(task_id='validate_tables')
def validate_tables(verification_results: List[Dict]) -> List[Dict]:
    """
    Valide les résultats de vérification et décide de continuer ou non.

    Comportement FAIL-FAST : si une seule table est en erreur,
    le DAG entier s'arrête.

    Args:
        verification_results: Liste des résultats de verify_table()

    Returns:
        Liste des tables validées (status == "success")

    Raises:
        AirflowException: Si au moins une table est en erreur
    """
    errors = []
    validated = []

    for result in verification_results:
        table_name = result.get('table_name', 'unknown')

        if result.get('status') == 'error':
            errors.append({
                'table': table_name,
                'phase': result.get('phase', 'unknown'),
                'error': result.get('error')
            })
        else:
            validated.append(result)

    if errors:
        logger.error(f"[VALIDATE] {len(errors)} erreur(s) détectée(s)")
        for err in errors:
            logger.error(f"  {err['table']} ({err['phase']}): {err['error']}")
        detail_lines = [
            f"- {err['table']} ({err['phase']}): {err['error']}"
            for err in errors
        ]
        details = "\n".join(detail_lines)
        raise AirflowException(
            f"Validation echouee: {len(errors)} table(s) en erreur\n{details}"
        )

    logger.info(f"[VALIDATE] {len(validated)} table(s) validée(s)")
    return validated
