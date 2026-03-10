"""
Tests unitaires pour AMUEPollingService
"""
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime


class TestPollingServiceConfig:
    """Tests pour la configuration du service de polling"""

    @patch('amue.services.api.polling_service.VarMgr')
    def test_load_default_config(self, mock_varmgr):
        """Charge la configuration par défaut depuis les variables Airflow"""
        mock_varmgr.get.side_effect = lambda key, default=None: {
            'amue_polling_interval_minutes': '10',
            'amue_max_wait_hours': '6',
            'amue_polling_exponential_backoff': 'False',
            'amue_polling_max_backoff_minutes': '60'
        }.get(key, default)

        from amue.services.api.polling_service import AMUEPollingService, PollingConfig

        mock_status_checker = MagicMock()
        service = AMUEPollingService(mock_status_checker)

        assert service.config.interval_minutes == 10
        assert service.config.max_wait_hours == 6
        assert service.config.exponential_backoff is False
        assert service.config.max_backoff_minutes == 60

    @patch('amue.services.api.polling_service.VarMgr')
    def test_custom_config(self, mock_varmgr):
        """Utilise une configuration personnalisée"""
        from amue.services.api.polling_service import AMUEPollingService, PollingConfig

        mock_status_checker = MagicMock()
        custom_config = PollingConfig(
            interval_minutes=5,
            max_wait_hours=2,
            exponential_backoff=True,
            max_backoff_minutes=30
        )

        service = AMUEPollingService(mock_status_checker, config=custom_config)

        assert service.config.interval_minutes == 5
        assert service.config.max_wait_hours == 2
        assert service.config.exponential_backoff is True
        assert service.config.max_backoff_minutes == 30


class TestPollingServiceCalculations:
    """Tests pour les calculs du service de polling"""

    @patch('amue.services.api.polling_service.VarMgr')
    def test_calculate_max_attempts(self, mock_varmgr):
        """Calcul du nombre maximum de tentatives"""
        from amue.services.api.polling_service import AMUEPollingService, PollingConfig

        mock_status_checker = MagicMock()
        config = PollingConfig(
            interval_minutes=10,
            max_wait_hours=6
        )

        service = AMUEPollingService(mock_status_checker, config=config)

        # 6 heures = 360 minutes / 10 minutes = 36 tentatives
        assert service._calculate_max_attempts() == 36

    @patch('amue.services.api.polling_service.VarMgr')
    def test_calculate_max_attempts_minimum_one(self, mock_varmgr):
        """Le nombre de tentatives doit être au moins 1"""
        from amue.services.api.polling_service import AMUEPollingService, PollingConfig

        mock_status_checker = MagicMock()
        config = PollingConfig(
            interval_minutes=120,  # 2 heures
            max_wait_hours=1  # 1 heure
        )

        service = AMUEPollingService(mock_status_checker, config=config)

        # Même si le calcul donne 0, on doit avoir au moins 1 tentative
        assert service._calculate_max_attempts() >= 1

    @patch('amue.services.api.polling_service.VarMgr')
    def test_calculate_wait_time_fixed(self, mock_varmgr):
        """Calcul du temps d'attente en mode fixe"""
        from amue.services.api.polling_service import AMUEPollingService, PollingConfig

        mock_status_checker = MagicMock()
        config = PollingConfig(
            interval_minutes=10,
            max_wait_hours=6,
            exponential_backoff=False
        )

        service = AMUEPollingService(mock_status_checker, config=config)

        # En mode fixe, toujours le même intervalle
        assert service._calculate_wait_time(1) == 10
        assert service._calculate_wait_time(5) == 10
        assert service._calculate_wait_time(10) == 10

    @patch('amue.services.api.polling_service.VarMgr')
    def test_calculate_wait_time_exponential(self, mock_varmgr):
        """Calcul du temps d'attente en mode exponentiel"""
        from amue.services.api.polling_service import AMUEPollingService, PollingConfig

        mock_status_checker = MagicMock()
        config = PollingConfig(
            interval_minutes=10,
            max_wait_hours=6,
            exponential_backoff=True,
            max_backoff_minutes=60
        )

        service = AMUEPollingService(mock_status_checker, config=config)

        # Tentative 1: 10 * 2^0 = 10 minutes
        assert service._calculate_wait_time(1) == 10
        # Tentative 2: 10 * 2^1 = 20 minutes
        assert service._calculate_wait_time(2) == 20
        # Tentative 3: 10 * 2^2 = 40 minutes
        assert service._calculate_wait_time(3) == 40
        # Tentative 4: 10 * 2^3 = 80 minutes, plafonné à 60
        assert service._calculate_wait_time(4) == 60

    @patch('amue.services.api.polling_service.VarMgr')
    def test_calculate_wait_time_exponential_capped(self, mock_varmgr):
        """Le backoff exponentiel est plafonné"""
        from amue.services.api.polling_service import AMUEPollingService, PollingConfig

        mock_status_checker = MagicMock()
        config = PollingConfig(
            interval_minutes=10,
            max_wait_hours=24,
            exponential_backoff=True,
            max_backoff_minutes=30
        )

        service = AMUEPollingService(mock_status_checker, config=config)

        # Même avec des tentatives élevées, plafonné à max_backoff_minutes
        assert service._calculate_wait_time(10) == 30


class TestPollingServiceErrorDetection:
    """Tests pour la détection d'erreurs"""

    @patch('amue.services.api.polling_service.VarMgr')
    def test_is_critical_error_4xx(self, mock_varmgr):
        """Les erreurs 4xx (sauf 429) sont critiques"""
        from amue.services.api.polling_service import AMUEPollingService, PollingConfig

        mock_status_checker = MagicMock()
        config = PollingConfig(interval_minutes=10, max_wait_hours=1)

        service = AMUEPollingService(mock_status_checker, config=config)

        assert service._is_critical_error(400) is True
        assert service._is_critical_error(401) is True
        assert service._is_critical_error(403) is True
        assert service._is_critical_error(404) is True

    @patch('amue.services.api.polling_service.VarMgr')
    def test_is_critical_error_429_not_critical(self, mock_varmgr):
        """Le code 429 (Too Many Requests) n'est pas critique"""
        from amue.services.api.polling_service import AMUEPollingService, PollingConfig

        mock_status_checker = MagicMock()
        config = PollingConfig(interval_minutes=10, max_wait_hours=1)

        service = AMUEPollingService(mock_status_checker, config=config)

        assert service._is_critical_error(429) is False

    @patch('amue.services.api.polling_service.VarMgr')
    def test_is_critical_error_5xx_not_critical(self, mock_varmgr):
        """Les erreurs 5xx ne sont pas critiques (retry possible)"""
        from amue.services.api.polling_service import AMUEPollingService, PollingConfig

        mock_status_checker = MagicMock()
        config = PollingConfig(interval_minutes=10, max_wait_hours=1)

        service = AMUEPollingService(mock_status_checker, config=config)

        assert service._is_critical_error(500) is False
        assert service._is_critical_error(502) is False
        assert service._is_critical_error(503) is False

    @patch('amue.services.api.polling_service.VarMgr')
    def test_is_server_error(self, mock_varmgr):
        """Détection des erreurs serveur (5xx)"""
        from amue.services.api.polling_service import AMUEPollingService, PollingConfig

        mock_status_checker = MagicMock()
        config = PollingConfig(interval_minutes=10, max_wait_hours=1)

        service = AMUEPollingService(mock_status_checker, config=config)

        assert service._is_server_error(500) is True
        assert service._is_server_error(502) is True
        assert service._is_server_error(503) is True
        assert service._is_server_error(599) is True
        assert service._is_server_error(200) is False
        assert service._is_server_error(400) is False


class TestPollingServiceElapsedTime:
    """Tests pour le calcul du temps écoulé"""

    @patch('amue.services.api.polling_service.VarMgr')
    def test_get_elapsed_time_no_start(self, mock_varmgr):
        """Sans start_time, retourne 0m 0s"""
        from amue.services.api.polling_service import AMUEPollingService, PollingConfig

        mock_status_checker = MagicMock()
        config = PollingConfig(interval_minutes=10, max_wait_hours=1)

        service = AMUEPollingService(mock_status_checker, config=config)
        service.start_time = None

        assert service._get_elapsed_time() == "0m 0s"

    @patch('amue.services.api.polling_service.VarMgr')
    def test_get_elapsed_time_with_start(self, mock_varmgr):
        """Avec start_time, retourne le temps formaté"""
        from amue.services.api.polling_service import AMUEPollingService, PollingConfig
        from datetime import timedelta

        mock_status_checker = MagicMock()
        config = PollingConfig(interval_minutes=10, max_wait_hours=1)

        service = AMUEPollingService(mock_status_checker, config=config)
        service.start_time = datetime.now() - timedelta(minutes=5, seconds=30)

        elapsed = service._get_elapsed_time()
        # Devrait être environ "5m 30s" (avec une petite marge d'erreur)
        assert "5m" in elapsed


class TestPollingServiceWaitForReady:
    """Tests pour wait_for_ready (utilise fetch_full_status)"""

    @patch('amue.services.admin_state_manager.AdminStateManager')
    @patch('amue.services.api.polling_service.VarMgr')
    @patch('amue.services.api.polling_service.time.sleep')
    def test_wait_for_ready_immediate_success(self, mock_sleep, mock_varmgr, mock_admin_cls):
        """API prête immédiatement (première exécution ou nouveau timestamp)"""
        mock_admin = MagicMock()
        mock_admin_cls.return_value = mock_admin
        mock_admin.get_last_finish_timestamp.return_value = None

        # Simule une première exécution (pas de timestamp précédent)
        mock_varmgr.get.side_effect = lambda key, default=None: {
            'amue_force_import': 'false'
        }.get(key, default)

        from amue.services.api.polling_service import AMUEPollingService, PollingConfig

        mock_status_checker = MagicMock()
        mock_status_checker.fetch_full_status.return_value = {
            'http_status': 200,
            'finish': '2024-01-15T10:00:00',
            'tables_status': {'CSKS': {'status': 'OK', 'mode': 'FULL', 'count': 1000, 'row_size': 100}}
        }

        config = PollingConfig(interval_minutes=10, max_wait_hours=1)
        service = AMUEPollingService(mock_status_checker, config=config)

        result = service.wait_for_ready()

        assert result['ready'] is True
        assert result['status'] == 'success'
        assert result['attempts'] == 1
        assert result['finish'] == '2024-01-15T10:00:00'
        # Vérifie que tables_status est inclus dans le résultat
        assert 'tables_status' in result
        assert 'CSKS' in result['tables_status']
        mock_sleep.assert_not_called()
        # Un seul appel à fetch_full_status (au lieu de 2 appels séparés)
        mock_status_checker.fetch_full_status.assert_called_once()

    @patch('amue.services.admin_state_manager.AdminStateManager')
    @patch('amue.services.api.polling_service.VarMgr')
    @patch('amue.services.api.polling_service.time.sleep')
    def test_wait_for_ready_after_retries(self, mock_sleep, mock_varmgr, mock_admin_cls):
        """API prête après plusieurs tentatives"""
        mock_admin = MagicMock()
        mock_admin_cls.return_value = mock_admin
        mock_admin.get_last_finish_timestamp.return_value = None

        # Simule première exécution
        mock_varmgr.get.side_effect = lambda key, default=None: {
            'amue_force_import': 'false'
        }.get(key, default)

        from amue.services.api.polling_service import AMUEPollingService, PollingConfig

        mock_status_checker = MagicMock()
        # Première tentative: 503, deuxième: 200 sans finish, troisième: 200 avec finish
        mock_status_checker.fetch_full_status.side_effect = [
            {'http_status': 503, 'finish': None, 'tables_status': {}},
            {'http_status': 200, 'finish': None, 'tables_status': {}},
            {'http_status': 200, 'finish': '2024-01-15T10:00:00',
             'tables_status': {'CSKS': {'status': 'OK', 'mode': 'FULL', 'count': 1000, 'row_size': 100}}}
        ]

        config = PollingConfig(interval_minutes=1, max_wait_hours=1)  # Intervalle court pour les tests
        service = AMUEPollingService(mock_status_checker, config=config)

        result = service.wait_for_ready()

        assert result['ready'] is True
        assert result['attempts'] == 3
        assert mock_sleep.call_count == 2  # Deux attentes entre les tentatives
        # 3 appels à fetch_full_status (un par tentative)
        assert mock_status_checker.fetch_full_status.call_count == 3

    @patch('amue.services.admin_state_manager.AdminStateManager')
    @patch('amue.services.api.polling_service.VarMgr')
    @patch('amue.services.api.polling_service.time.sleep')
    def test_wait_for_ready_critical_error(self, mock_sleep, mock_varmgr, mock_admin_cls):
        """Erreur critique arrête le polling"""
        mock_admin = MagicMock()
        mock_admin_cls.return_value = mock_admin
        mock_admin.get_last_finish_timestamp.return_value = None

        mock_varmgr.get.side_effect = lambda key, default=None: {
            'amue_force_import': 'false'
        }.get(key, default)

        from amue.services.api.polling_service import AMUEPollingService, PollingConfig
        from airflow.exceptions import AirflowException

        mock_status_checker = MagicMock()
        mock_status_checker.fetch_full_status.return_value = {
            'http_status': 401,  # Erreur critique
            'finish': None,
            'tables_status': {}
        }

        config = PollingConfig(interval_minutes=10, max_wait_hours=1)
        service = AMUEPollingService(mock_status_checker, config=config)

        with pytest.raises(AirflowException, match="Code HTTP critique 401"):
            service.wait_for_ready()

    @patch('amue.services.admin_state_manager.AdminStateManager')
    @patch('amue.services.api.polling_service.VarMgr')
    @patch('amue.services.api.polling_service.time.sleep')
    def test_wait_for_ready_timeout(self, mock_sleep, mock_varmgr, mock_admin_cls):
        """Timeout après max_wait_hours"""
        mock_admin = MagicMock()
        mock_admin_cls.return_value = mock_admin
        mock_admin.get_last_finish_timestamp.return_value = None

        mock_varmgr.get.side_effect = lambda key, default=None: {
            'amue_force_import': 'false'
        }.get(key, default)

        from amue.services.api.polling_service import AMUEPollingService, PollingConfig
        from airflow.exceptions import AirflowException

        mock_status_checker = MagicMock()
        mock_status_checker.fetch_full_status.return_value = {
            'http_status': 200,
            'finish': None,  # Jamais prêt
            'tables_status': {}
        }

        # Config avec intervalle de 30min et max 1h = 2 tentatives max
        config = PollingConfig(interval_minutes=30, max_wait_hours=1)
        service = AMUEPollingService(mock_status_checker, config=config)

        with pytest.raises(AirflowException, match="Timeout"):
            service.wait_for_ready()


class TestPollingResult:
    """Tests pour PollingResult dataclass"""

    def test_polling_result_success(self):
        """Création d'un résultat de succès"""
        from amue.services.api.polling_service import PollingResult

        result = PollingResult(
            ready=True,
            attempts=3,
            total_wait_minutes=15.5,
            status='success',
            last_status_code=200
        )

        assert result.ready is True
        assert result.attempts == 3
        assert result.total_wait_minutes == 15.5
        assert result.status == 'success'
        assert result.last_status_code == 200
        assert result.error is None

    def test_polling_result_timeout(self):
        """Création d'un résultat de timeout"""
        from amue.services.api.polling_service import PollingResult

        result = PollingResult(
            ready=False,
            attempts=36,
            total_wait_minutes=360.0,
            status='timeout',
            last_status_code=200,
            error='Timeout après 6 heures'
        )

        assert result.ready is False
        assert result.status == 'timeout'
        assert result.error == 'Timeout après 6 heures'

    def test_polling_result_no_skip_fields(self):
        """PollingResult n'a plus de champs skip_import/skip_reason"""
        from amue.services.api.polling_service import PollingResult

        result = PollingResult(
            ready=True,
            attempts=1,
            total_wait_minutes=0.5,
            status='success',
            last_status_code=200
        )

        assert not hasattr(result, 'skip_import')
        assert not hasattr(result, 'skip_reason')


class TestPollingServiceSkipImport:
    """Tests pour la détection de skip (même timestamp finish)"""

    @patch('amue.services.admin_state_manager.AdminStateManager')
    @patch('amue.services.api.polling_service.VarMgr')
    def test_should_skip_import_same_timestamp(self, mock_varmgr, mock_admin_cls):
        """Skip si même timestamp finish"""
        mock_admin = MagicMock()
        mock_admin_cls.return_value = mock_admin
        mock_admin.get_last_finish_timestamp.return_value = '2024-01-15T10:00:00'

        mock_varmgr.get.side_effect = lambda key, default=None: {
            'amue_polling_interval_minutes': '10',
            'amue_max_wait_hours': '6',
            'amue_polling_exponential_backoff': 'False',
            'amue_polling_max_backoff_minutes': '60',
            'amue_force_import': 'false'
        }.get(key, default)

        from amue.services.api.polling_service import AMUEPollingService, PollingConfig

        mock_status_checker = MagicMock()
        service = AMUEPollingService(mock_status_checker)

        assert service._should_skip_import('2024-01-15T10:00:00') is True

    @patch('amue.services.admin_state_manager.AdminStateManager')
    @patch('amue.services.api.polling_service.VarMgr')
    def test_should_not_skip_import_greater_timestamp(self, mock_varmgr, mock_admin_cls):
        """Ne skip pas si timestamp strictement supérieur (nouvelles données)"""
        mock_admin = MagicMock()
        mock_admin_cls.return_value = mock_admin
        mock_admin.get_last_finish_timestamp.return_value = '2024-01-15T10:00:00'

        mock_varmgr.get.side_effect = lambda key, default=None: {
            'amue_polling_interval_minutes': '10',
            'amue_max_wait_hours': '6',
            'amue_polling_exponential_backoff': 'False',
            'amue_polling_max_backoff_minutes': '60',
            'amue_force_import': 'false'
        }.get(key, default)

        from amue.services.api.polling_service import AMUEPollingService, PollingConfig

        mock_status_checker = MagicMock()
        service = AMUEPollingService(mock_status_checker)

        assert service._should_skip_import('2024-01-16T10:00:00') is False

    @patch('amue.services.admin_state_manager.AdminStateManager')
    @patch('amue.services.api.polling_service.VarMgr')
    def test_should_skip_import_inferior_timestamp(self, mock_varmgr, mock_admin_cls):
        """Skip si timestamp inférieur au précédent (cas anormal)"""
        mock_admin = MagicMock()
        mock_admin_cls.return_value = mock_admin
        mock_admin.get_last_finish_timestamp.return_value = '2024-01-15T10:00:00'

        mock_varmgr.get.side_effect = lambda key, default=None: {
            'amue_polling_interval_minutes': '10',
            'amue_max_wait_hours': '6',
            'amue_polling_exponential_backoff': 'False',
            'amue_polling_max_backoff_minutes': '60',
            'amue_force_import': 'false'
        }.get(key, default)

        from amue.services.api.polling_service import AMUEPollingService, PollingConfig

        mock_status_checker = MagicMock()
        service = AMUEPollingService(mock_status_checker)

        # Timestamp antérieur → cas anormal, doit être ignoré
        assert service._should_skip_import('2024-01-14T10:00:00') is True

    @patch('amue.services.admin_state_manager.AdminStateManager')
    @patch('amue.services.api.polling_service.VarMgr')
    def test_should_not_skip_import_no_previous(self, mock_varmgr, mock_admin_cls):
        """Ne skip pas si pas de timestamp précédent (première exécution)"""
        mock_admin = MagicMock()
        mock_admin_cls.return_value = mock_admin
        mock_admin.get_last_finish_timestamp.return_value = None

        mock_varmgr.get.side_effect = lambda key, default=None: {
            'amue_polling_interval_minutes': '10',
            'amue_max_wait_hours': '6',
            'amue_polling_exponential_backoff': 'False',
            'amue_polling_max_backoff_minutes': '60',
            'amue_force_import': 'false'
        }.get(key, default)

        from amue.services.api.polling_service import AMUEPollingService, PollingConfig

        mock_status_checker = MagicMock()
        service = AMUEPollingService(mock_status_checker)

        assert service._should_skip_import('2024-01-15T10:00:00') is False

    @patch('amue.services.admin_state_manager.AdminStateManager')
    @patch('amue.services.api.polling_service.VarMgr')
    def test_should_not_skip_import_force_enabled(self, mock_varmgr, mock_admin_cls):
        """Ne skip jamais si force_import activé"""
        mock_admin = MagicMock()
        mock_admin_cls.return_value = mock_admin
        mock_admin.get_last_finish_timestamp.return_value = '2024-01-15T10:00:00'

        mock_varmgr.get.side_effect = lambda key, default=None: {
            'amue_polling_interval_minutes': '10',
            'amue_max_wait_hours': '6',
            'amue_polling_exponential_backoff': 'False',
            'amue_polling_max_backoff_minutes': '60',
            'amue_force_import': 'true'  # Force activé
        }.get(key, default)

        from amue.services.api.polling_service import AMUEPollingService, PollingConfig

        mock_status_checker = MagicMock()
        service = AMUEPollingService(mock_status_checker)

        # Même timestamp mais force_import=true -> pas de skip
        assert service._should_skip_import('2024-01-15T10:00:00') is False

    @patch('amue.services.api.polling_service.VarMgr')
    def test_validate_finish_timestamp_valid(self, mock_varmgr):
        """Valide un timestamp correct"""
        from amue.services.api.polling_service import AMUEPollingService, PollingConfig

        mock_status_checker = MagicMock()
        config = PollingConfig(interval_minutes=10, max_wait_hours=1)
        service = AMUEPollingService(mock_status_checker, config=config)

        assert service._validate_finish_timestamp('2024-01-15T10:00:00') is True
        assert service._validate_finish_timestamp('20240115100000') is True

    @patch('amue.services.api.polling_service.VarMgr')
    def test_validate_finish_timestamp_invalid(self, mock_varmgr):
        """Rejette les timestamps invalides"""
        from amue.services.api.polling_service import AMUEPollingService, PollingConfig

        mock_status_checker = MagicMock()
        config = PollingConfig(interval_minutes=10, max_wait_hours=1)
        service = AMUEPollingService(mock_status_checker, config=config)

        assert service._validate_finish_timestamp('') is False
        assert service._validate_finish_timestamp('   ') is False
        assert service._validate_finish_timestamp('null') is False
        assert service._validate_finish_timestamp('none') is False
        assert service._validate_finish_timestamp('0') is False

    @patch('amue.services.admin_state_manager.AdminStateManager')
    @patch('amue.services.api.polling_service.VarMgr')
    def test_should_not_skip_import_invalid_timestamp(self, mock_varmgr, mock_admin_cls):
        """Ne skip pas si timestamp actuel invalide (sécurité)"""
        mock_admin = MagicMock()
        mock_admin_cls.return_value = mock_admin
        mock_admin.get_last_finish_timestamp.return_value = '2024-01-15T10:00:00'

        mock_varmgr.get.side_effect = lambda key, default=None: {
            'amue_polling_interval_minutes': '10',
            'amue_max_wait_hours': '6',
            'amue_polling_exponential_backoff': 'False',
            'amue_polling_max_backoff_minutes': '60',
            'amue_force_import': 'false'
        }.get(key, default)

        from amue.services.api.polling_service import AMUEPollingService, PollingConfig

        mock_status_checker = MagicMock()
        service = AMUEPollingService(mock_status_checker)

        # Timestamp invalide -> import par précaution
        assert service._should_skip_import('') is False
        assert service._should_skip_import('null') is False

    @patch('amue.services.admin_state_manager.AdminStateManager')
    @patch('amue.services.api.polling_service.VarMgr')
    @patch('amue.services.api.polling_service.time.sleep')
    def test_wait_for_ready_continues_polling_same_timestamp(self, mock_sleep, mock_varmgr, mock_admin_cls):
        """wait_for_ready continue le polling si même timestamp (au lieu de skip)"""
        mock_admin = MagicMock()
        mock_admin_cls.return_value = mock_admin
        mock_admin.get_last_finish_timestamp.return_value = '2024-01-15T10:00:00'

        mock_varmgr.get.side_effect = lambda key, default=None: {
            'amue_polling_interval_minutes': '30',
            'amue_max_wait_hours': '1',
            'amue_polling_exponential_backoff': 'False',
            'amue_polling_max_backoff_minutes': '60',
            'amue_force_import': 'false'
        }.get(key, default)

        from amue.services.api.polling_service import AMUEPollingService, PollingConfig
        from airflow.exceptions import AirflowException

        mock_status_checker = MagicMock()
        mock_status_checker.fetch_full_status.return_value = {
            'http_status': 200,
            'finish': '2024-01-15T10:00:00',  # Même timestamp
            'tables_status': {'CSKS': {'status': 'OK', 'mode': 'FULL', 'count': 1000, 'row_size': 100}}
        }

        service = AMUEPollingService(mock_status_checker)

        # Avec le même timestamp, le polling continue jusqu'au timeout
        with pytest.raises(AirflowException, match="Timeout"):
            service.wait_for_ready()

    @patch('amue.services.admin_state_manager.AdminStateManager')
    @patch('amue.services.api.polling_service.VarMgr')
    @patch('amue.services.api.polling_service.time.sleep')
    def test_wait_for_ready_first_execution(self, mock_sleep, mock_varmgr, mock_admin_cls):
        """wait_for_ready exécute l'import lors de la première exécution"""
        mock_admin = MagicMock()
        mock_admin_cls.return_value = mock_admin
        mock_admin.get_last_finish_timestamp.return_value = None

        mock_varmgr.get.side_effect = lambda key, default=None: {
            'amue_polling_interval_minutes': '10',
            'amue_max_wait_hours': '6',
            'amue_polling_exponential_backoff': 'False',
            'amue_polling_max_backoff_minutes': '60',
            'amue_force_import': 'false'
        }.get(key, default)

        from amue.services.api.polling_service import AMUEPollingService, PollingConfig

        mock_status_checker = MagicMock()
        mock_status_checker.fetch_full_status.return_value = {
            'http_status': 200,
            'finish': '2024-01-15T10:00:00',
            'tables_status': {'CSKS': {'status': 'OK', 'mode': 'FULL', 'count': 1000, 'row_size': 100}}
        }

        service = AMUEPollingService(mock_status_checker)

        result = service.wait_for_ready()

        assert result['ready'] is True
        assert result['status'] == 'success'
        # Vérifie que tables_status est inclus
        assert 'tables_status' in result
        mock_sleep.assert_not_called()


class TestPollingConfig:
    """Tests pour PollingConfig dataclass"""

    def test_polling_config_defaults(self):
        """Valeurs par défaut de PollingConfig"""
        from amue.services.api.polling_service import PollingConfig

        config = PollingConfig(
            interval_minutes=10,
            max_wait_hours=6
        )

        assert config.interval_minutes == 10
        assert config.max_wait_hours == 6
        assert config.exponential_backoff is False
        assert config.max_backoff_minutes == 60

    def test_polling_config_custom(self):
        """Configuration personnalisée"""
        from amue.services.api.polling_service import PollingConfig

        config = PollingConfig(
            interval_minutes=5,
            max_wait_hours=2,
            exponential_backoff=True,
            max_backoff_minutes=30
        )

        assert config.interval_minutes == 5
        assert config.max_wait_hours == 2
        assert config.exponential_backoff is True
        assert config.max_backoff_minutes == 30


class TestFinishTimestampValidatorNormalize:
    """
    Tests de normalisation des timestamps.

    Régression : la comparaison brute de chaînes échouait quand l'API retourne
    '2026-03-09 10:00:00' (espace) mais la DB retourne '2026-03-09T10:00:00' (T).
    """

    def test_normalize_space_separator(self):
        """Espace → T."""
        from amue.services.api.finish_timestamp_validator import FinishTimestampValidator
        assert FinishTimestampValidator._normalize_ts('2026-03-09 10:00:00') == '2026-03-09T10:00:00'

    def test_normalize_t_separator_unchanged(self):
        """T reste T."""
        from amue.services.api.finish_timestamp_validator import FinishTimestampValidator
        assert FinishTimestampValidator._normalize_ts('2026-03-09T10:00:00') == '2026-03-09T10:00:00'

    def test_normalize_strips_timezone_offset(self):
        """Timezone +00:00 supprimé."""
        from amue.services.api.finish_timestamp_validator import FinishTimestampValidator
        assert FinishTimestampValidator._normalize_ts('2026-03-09T10:00:00+00:00') == '2026-03-09T10:00:00'

    def test_normalize_strips_z_suffix(self):
        """Timezone Z supprimé."""
        from amue.services.api.finish_timestamp_validator import FinishTimestampValidator
        assert FinishTimestampValidator._normalize_ts('2026-03-09T10:00:00Z') == '2026-03-09T10:00:00'

    def test_normalize_strips_microseconds(self):
        """Microsecondes supprimées."""
        from amue.services.api.finish_timestamp_validator import FinishTimestampValidator
        assert FinishTimestampValidator._normalize_ts('2026-03-09T10:00:00.123456') == '2026-03-09T10:00:00'

    def test_normalize_empty_string(self):
        """Chaîne vide retourne vide."""
        from amue.services.api.finish_timestamp_validator import FinishTimestampValidator
        assert FinishTimestampValidator._normalize_ts('') == ''

    @patch('amue.services.admin_state_manager.AdminStateManager')
    def test_should_skip_detects_same_timestamp_despite_format_mismatch(self, mock_admin_cls):
        """
        Régression : API retourne espace, DB retourne T — doit quand même détecter INCHANGÉ.

        Avant le fix, cette comparaison retournait False (import lancé à tort).
        """
        from amue.services.api.finish_timestamp_validator import FinishTimestampValidator

        mock_admin = MagicMock()
        mock_admin_cls.return_value = mock_admin
        # DB retourne via datetime.isoformat() : format avec T
        mock_admin.get_last_finish_timestamp.return_value = '2026-03-09T10:00:00'

        validator = FinishTimestampValidator()
        # API retourne format avec espace
        result = validator.should_skip('2026-03-09 10:00:00')

        assert result is True, "Même timestamp (format différent) doit déclencher le skip"

    @patch('amue.services.admin_state_manager.AdminStateManager')
    def test_should_not_skip_newer_timestamp_despite_format_mismatch(self, mock_admin_cls):
        """Timestamp plus récent côté API → import doit être lancé, quel que soit le format."""
        from amue.services.api.finish_timestamp_validator import FinishTimestampValidator

        mock_admin = MagicMock()
        mock_admin_cls.return_value = mock_admin
        mock_admin.get_last_finish_timestamp.return_value = '2026-03-09T10:00:00'

        validator = FinishTimestampValidator()
        result = validator.should_skip('2026-03-10 08:00:00')

        assert result is False, "Timestamp plus récent ne doit pas déclencher le skip"
