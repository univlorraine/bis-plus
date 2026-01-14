"""
Gestionnaire de vérification des statuts AMUE
Mise à jour avec vérification de la variable 'finish'
"""
from datetime import datetime, timedelta
from string import Template
from typing import Dict, List, Optional
from airflow.exceptions import AirflowException
from amue.utils.airflow_helpers import AirflowVariableManager as VarMgr


class AMUEStatusChecker:
    """Gère la vérification des statuts historiques et actuels de l'API AMUE"""

    def __init__(self, api_hook):
        self.api_hook = api_hook
        try:
            univ = VarMgr.get('universite')
        except KeyError:
            raise AirflowException("La variable 'univ' doit être définie pour initialiser AMUEStatusChecker")
        try:
            endpointadm = VarMgr.get('api_endpoint_admin')
        except KeyError:
            raise AirflowException("La variable 'api_endpoint_admin' doit être définie pour initialiser AMUEStatusChecker")
        try:
            self.endpoint = Template(endpointadm).substitute(univ=univ)
        except KeyError as e:
            raise AirflowException(f"Erreur lors de la substitution dans l'endpoint: {e}")

    def check_historical_status(self, max_days: int = 7) -> Dict:
        """Vérifie les statuts historiques sur N jours"""
        print(f"[HISTORY] Vérification sur {max_days} jours")

        last_success_date = self._get_last_success_date()
        days_to_check = self._compute_days_to_check(last_success_date, max_days)

        print(f"[HISTORY] Dernière exécution réussie: {last_success_date}")
        print(f"[HISTORY] Jours à vérifier: {[str(d) for d in days_to_check]}")

        status_by_date = {}

        for date_to_check in days_to_check:
            date_str = date_to_check.strftime('%Y%m%d')
            status_info = self._fetch_status_for_date(date_str)
            status_by_date[date_str] = status_info

            if 'error' not in status_info:
                print(f"[HISTORY] {date_str}: {len(status_info.get('tables_status', {}))} tables, "
                      f"KO: {status_info.get('nbtables_ko', 0)}")

        return {
            'status_by_date': self._serialize_dates(status_by_date),
            'dates_checked': [str(d) for d in days_to_check]
        }

    def get_current_status(self) -> Dict:
        """Récupère le statut actuel de l'API"""
        print("[STATUS] Récupération statut actuel")

        params = {'status': ''}
        response = self.api_hook.call_api(self.endpoint, params)

        if not isinstance(response, dict) or 'status' not in response:
            raise ValueError("Format réponse invalide")

        tables_status = self._parse_tables_status(response.get('status', []))
        print(f"[STATUS] {len(tables_status)} tables trouvées")

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
        print("[STATUS] Vérification variable 'finish'")

        try:
            params = {'status': ''}
            response = self.api_hook.call_api(self.endpoint, params)

            if not isinstance(response, dict):
                print("[WARN] Réponse non-JSON lors de la vérification 'finish'")
                return None

            finish_value = response.get('finish')

            if finish_value:
                print(f"[STATUS] Variable 'finish' trouvée: {finish_value}")
                return finish_value
            else:
                print("[STATUS] Variable 'finish' non renseignée (traitement en cours)")
                return None

        except Exception as e:
            print(f"[ERROR] Erreur lors de la vérification 'finish': {str(e)}")
            raise AirflowException(f"Impossible de vérifier 'finish': {str(e)}")

    def _get_last_success_date(self) -> datetime.date:
        """Récupère la date du dernier succès"""
        try:
            last_success_str = VarMgr.get('amue_last_successful_run', default='')
            if last_success_str:
                return datetime.fromisoformat(last_success_str).date()
        except:
            pass

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
        """Récupère le statut pour une date donnée"""
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
            print(f"[ERROR] Erreur vérification {date_str}: {e}")
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