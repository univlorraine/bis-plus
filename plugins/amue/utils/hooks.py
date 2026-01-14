# amue/utils/hooks.py
from airflow.providers.postgres.hooks.postgres import PostgresHook
from amue.hooks.amue_api_hook import AMUEAPIHook


class HookManager:
    """Gestionnaire centralisé de hooks (singleton)"""

    _instance = None
    _api_hook = None
    _postgres_hook = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    @property
    def api_hook(self) -> AMUEAPIHook:
        if self._api_hook is None:
            self._api_hook = AMUEAPIHook()
        return self._api_hook

    @property
    def postgres_hook(self) -> PostgresHook:
        if self._postgres_hook is None:
            self._postgres_hook = PostgresHook(
                postgres_conn_id='postgres_data',
                options='-c search_path=splus'
            )
        return self._postgres_hook