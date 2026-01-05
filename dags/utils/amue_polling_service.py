"""
Service de polling pour attendre la disponibilité de l'API
"""
import time
from typing import Dict
from airflow.sdk import Variable
from airflow.exceptions import AirflowException


class AMUEPollingService:
    """Gère le polling pour attendre que l'API soit prête"""

    def __init__(self, status_checker):
        self.status_checker = status_checker
        self.polling_interval = int(Variable.get('amue_polling_interval_minutes', default='10'))
        self.max_wait_hours = int(Variable.get('amue_max_wait_hours', default='6'))

    def wait_for_ready(self) -> Dict:
        """Attend que l'API retourne un code 200"""
        print(f"[POLLING] Démarrage polling")
        print(f"[POLLING] Intervalle: {self.polling_interval}min, Max: {self.max_wait_hours}h")

        max_attempts = (self.max_wait_hours * 60) // self.polling_interval
        attempt = 0

        while attempt < max_attempts:
            attempt += 1
            print(f"[POLLING] Tentative {attempt}/{max_attempts}")

            try:
                status_code = self.status_checker.check_status_code()
                print(f"[POLLING] Code HTTP: {status_code}")

                if status_code == 200:
                    print("[POLLING] Code 200 reçu!")
                    return {
                        'ready': True,
                        'attempts': attempt,
                        'total_wait_minutes': attempt * self.polling_interval,
                        'status': 'success'
                    }

                if attempt < max_attempts:
                    print(f"[POLLING] Attente {self.polling_interval}min...")
                    time.sleep(self.polling_interval * 60)

            except Exception as e:
                print(f"[ERROR] Erreur polling: {e}")
                if attempt < max_attempts:
                    time.sleep(self.polling_interval * 60)

        error_msg = f"Timeout: pas prêt après {self.max_wait_hours}h"
        print(f"[ERROR] {error_msg}")
        raise AirflowException(error_msg)