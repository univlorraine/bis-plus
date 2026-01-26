"""
Gestionnaire de vérification des statuts AMUE.

================================================================================
RÔLE DU MODULE
================================================================================

Ce module interroge l'API AMUE pour récupérer le statut des tables disponibles.
Il est utilisé à deux moments :
    1. POLLING : Vérifier si l'API est accessible et le traitement terminé
    2. FILTRAGE : Récupérer la liste des tables disponibles et leur statut

================================================================================
STRUCTURE DE LA RÉPONSE API
================================================================================

L'API AMUE renvoie une réponse JSON avec la structure suivante :

{
    "finish": "2024-01-15 03:45:00",  # Date/heure de fin du traitement AMUE
    "nbtables": 25,                    # Nombre total de tables
    "nbtables_ko": 0,                  # Nombre de tables en erreur
    "status": [
        {
            "name": "CSKS",            # Nom de la table
            "status": "OK",            # Statut ("OK" ou "KO")
            "mode": "FULL",            # Mode d'export AMUE
            "count": 15000,            # Nombre de lignes
            "row_size": 256            # Taille moyenne d'une ligne
        },
        ...
    ]
}

================================================================================
VARIABLE 'FINISH'
================================================================================

La variable 'finish' est CRITIQUE pour le fonctionnement du DAG :
    - Si VIDE ou ABSENTE : Le traitement AMUE est EN COURS
    - Si RENSEIGNÉE : Le traitement AMUE est TERMINÉ, on peut importer

Le polling attend que 'finish' soit renseigné avant de continuer.

================================================================================
VÉRIFICATION HISTORIQUE
================================================================================

La méthode check_historical_status() permet de vérifier les statuts
sur plusieurs jours passés. C'est utile pour :
    - Identifier les imports manqués
    - Détecter les tables qui étaient en erreur
    - Calculer des métriques de disponibilité

================================================================================
CONFIGURATION
================================================================================

Variables Airflow :
    - universite : Code université pour l'endpoint
    - api_endpoint_admin : Template URL admin avec $univ
    - amue_last_successful_run : Date ISO du dernier succès (automatique)

================================================================================
"""
import logging
from datetime import datetime, timedelta
from string import Template
from typing import Dict, List, Optional

from airflow.exceptions import AirflowException
from amue.utils.airflow_helpers import AirflowVariableManager as VarMgr

logger = logging.getLogger(__name__)


class AMUEStatusChecker:
    """
    Gère la vérification des statuts historiques et actuels de l'API AMUE
    """

    def __init__(self, api_hook):
        """
        Initialise le vérificateur de statuts

        Args:
            api_hook: Hook API AMUE
        """
        self.api_hook = api_hook

        try:
            univ = VarMgr.get('universite')
        except KeyError:
            raise AirflowException("La variable 'univ' doit être définie pour initialiser AMUEStatusChecker")
        try:
            endpointadm = VarMgr.get('api_endpoint_admin')
        except KeyError:
            raise AirflowException(
                "La variable 'api_endpoint_admin' doit être définie pour initialiser AMUEStatusChecker")
        try:
            self.endpoint = Template(endpointadm).substitute(univ=univ)
        except KeyError as e:
            raise AirflowException(f"Erreur lors de la substitution dans l'endpoint: {e}")

    def check_historical_status(self, max_days: int = 7) -> Dict:
        """Vérifie les statuts historiques sur N jours"""
        logger.info(f"[HISTORY] Vérification sur {max_days} jours")

        last_success_date = self._get_last_success_date()
        days_to_check = self._compute_days_to_check(last_success_date, max_days)

        logger.info(f"[HISTORY] Dernière exécution réussie: {last_success_date}")
        logger.info(f"[HISTORY] Jours à vérifier: {[str(d) for d in days_to_check]}")

        status_by_date = {}

        for date_to_check in days_to_check:
            date_str = date_to_check.strftime('%Y%m%d')
            status_info = self._fetch_status_for_date(date_str)
            status_by_date[date_str] = status_info

            if 'error' not in status_info:
                logger.info(f"[HISTORY] {date_str}: {len(status_info.get('tables_status', {}))} tables, "
                            f"KO: {status_info.get('nbtables_ko', 0)}")

        return {
            'status_by_date': self._serialize_dates(status_by_date),
            'dates_checked': [str(d) for d in days_to_check]
        }

    def get_current_status(self) -> Dict:
        """
        Récupère le statut actuel de l'API

        Returns:
            Dictionnaire des statuts par table
        """
        logger.info("[STATUS] Récupération statut actuel (API)")

        params = {'status': ''}
        response = self.api_hook.call_api(self.endpoint, params)

        if not isinstance(response, dict) or 'status' not in response:
            raise ValueError("Format réponse invalide")

        tables_status = self._parse_tables_status(response.get('status', []))

        logger.info(f"[STATUS] {len(tables_status)} tables trouvées")

        return tables_status

    def check_status_code(self) -> int:
        """Vérifie uniquement le code HTTP (pour polling)"""
        params = {'status': ''}
        return self.api_hook.call_api(self.endpoint, params, check_status_only=True)

    def check_finish_status(self) -> Optional[str]:
        """
        Vérifie la variable 'finish' du JSON de statut

        Cette méthode est utilisée par le polling pour s'assurer que le traitement
        côté AMUE est terminé avant de continuer.

        Returns:
            Valeur de 'finish' si présente (date/heure de fin), None sinon

        Raises:
            AirflowException: Si erreur lors de la récupération
        """
        logger.info("[STATUS] Vérification variable 'finish'")

        try:
            params = {'status': ''}
            response = self.api_hook.call_api(self.endpoint, params)

            if not isinstance(response, dict):
                logger.warning("[WARN] Réponse non-JSON lors de la vérification 'finish'")
                return None

            finish_value = response.get('finish')

            if finish_value:
                logger.info(f"[STATUS] Variable 'finish' trouvée: {finish_value}")
                return finish_value
            else:
                logger.info("[STATUS] Variable 'finish' non renseignée (traitement en cours)")
                return None

        except Exception as e:
            logger.error(f"[ERROR] Erreur lors de la vérification 'finish': {str(e)}")
            raise AirflowException(f"Impossible de vérifier 'finish': {str(e)}")

    def _get_last_success_date(self) -> datetime.date:
        """Récupère la date du dernier succès"""
        try:
            last_success_str = VarMgr.get('amue_last_successful_run', default='')
            if last_success_str:
                return datetime.fromisoformat(last_success_str).date()
        except (ValueError, TypeError, AttributeError) as e:
            logger.warning(f"[WARN] Impossible de parser la date du dernier succès: {e}")

        return (datetime.now() - timedelta(days=1)).date()

    def _compute_days_to_check(self, last_success_date: datetime.date, max_days: int) -> List[datetime.date]:
        """Calcule la liste des jours à vérifier"""
        days_to_check = []
        check_date = datetime.now().date()

        for _ in range(max_days):
            if check_date <= last_success_date:
                break
            days_to_check.append(check_date)
            check_date = check_date - timedelta(days=1)

        return days_to_check

    def _fetch_status_for_date(self, date_str: str) -> Dict:
        """
        Récupère le statut pour une date donnée

        Args:
            date_str: Date au format YYYYMMDD

        Returns:
            Dictionnaire avec les statuts de la date
        """
        params = {'status': '', 'date': date_str}

        try:
            response = self.api_hook.call_api(self.endpoint, params)

            if not isinstance(response, dict):
                raise ValueError("Réponse invalide")

            tables_status = self._parse_tables_status(response.get('status', []))

            return {
                'date': datetime.strptime(date_str, '%Y%m%d').date(),
                'tables_status': tables_status,
                'finish': response.get('finish', ''),
                'nbtables': response.get('nbtables', 0),
                'nbtables_ko': response.get('nbtables_ko', 0)
            }

        except Exception as e:
            logger.error(f"[ERROR] Erreur vérification {date_str}: {e}")
            return {
                'date': datetime.strptime(date_str, '%Y%m%d').date(),
                'tables_status': {},
                'error': str(e)
            }

    def _parse_tables_status(self, status_list: List) -> Dict:
        """Parse la liste des statuts de tables"""
        tables_status = {}

        if not isinstance(status_list, list):
            return tables_status

        for table_info in status_list:
            if not isinstance(table_info, dict):
                continue

            table_name = table_info.get('name', '').upper()
            if not table_name:
                continue

            tables_status[table_name] = {
                'status': table_info.get('status', 'UNKNOWN'),
                'mode': table_info.get('mode', 'UNKNOWN'),
                'count': table_info.get('count', 0),
                'row_size': table_info.get('row_size', 0)
            }

        return tables_status

    def _serialize_dates(self, status_by_date: Dict) -> Dict:
        """Sérialise les dates pour JSON"""
        return {
            k: {
                **v,
                'date': v['date'].isoformat() if 'date' in v else None
            }
            for k, v in status_by_date.items()
        }
