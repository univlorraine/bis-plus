"""
Tests unitaires pour la configuration AMUEConfig
"""
import pytest

from amue.infrastructure.config.settings import AMUEConfig


class TestAMUEConfigValidation:
    """Tests pour la validation de AMUEConfig"""

    def _create_valid_config(self, **overrides):
        """Crée une configuration valide avec possibilité de surcharge"""
        defaults = {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.example.com/admin',
            'api_endpoint_table': 'https://api.example.com/table',
            'api_max_retries': 3,
            'api_retry_delay_seconds': 30,
            'polling_interval_minutes': 10,
            'polling_max_wait_hours': 6,
            'polling_exponential_backoff': False,
            'import_batch_size': 5000,
            'import_max_memory_mb': 512,
            'smtp_host': 'mailhog',
            'smtp_port': 1025,
            'smtp_from': 'airflow@amue.local',
            'report_recipients': ['admin@example.com'],
        }
        defaults.update(overrides)
        return AMUEConfig(**defaults)

    def test_valid_config(self):
        """Configuration valide ne doit pas lever d'erreur"""
        config = self._create_valid_config()
        assert config.universite == 'ul'
        assert config.api_max_retries == 3

    def test_invalid_universite_empty(self):
        """Université vide doit lever une erreur"""
        with pytest.raises(ValueError, match="universite"):
            self._create_valid_config(universite='')

    def test_invalid_universite_format(self):
        """Format d'université invalide doit lever une erreur"""
        with pytest.raises(ValueError, match="universite"):
            self._create_valid_config(universite='u')  # Trop court

        with pytest.raises(ValueError, match="universite"):
            self._create_valid_config(universite='univ@invalid')  # Caractères invalides

    def test_invalid_api_max_retries(self):
        """api_max_retries hors limites doit lever une erreur"""
        with pytest.raises(ValueError, match="api_max_retries"):
            self._create_valid_config(api_max_retries=0)

        with pytest.raises(ValueError, match="api_max_retries"):
            self._create_valid_config(api_max_retries=11)

    def test_invalid_api_retry_delay(self):
        """api_retry_delay_seconds hors limites doit lever une erreur"""
        with pytest.raises(ValueError, match="api_retry_delay_seconds"):
            self._create_valid_config(api_retry_delay_seconds=0)

        with pytest.raises(ValueError, match="api_retry_delay_seconds"):
            self._create_valid_config(api_retry_delay_seconds=301)

    def test_invalid_polling_interval(self):
        """polling_interval_minutes hors limites doit lever une erreur"""
        with pytest.raises(ValueError, match="polling_interval_minutes"):
            self._create_valid_config(polling_interval_minutes=0)

        with pytest.raises(ValueError, match="polling_interval_minutes"):
            self._create_valid_config(polling_interval_minutes=121)

    def test_invalid_polling_max_wait(self):
        """polling_max_wait_hours hors limites doit lever une erreur"""
        with pytest.raises(ValueError, match="polling_max_wait_hours"):
            self._create_valid_config(polling_max_wait_hours=0)

        with pytest.raises(ValueError, match="polling_max_wait_hours"):
            self._create_valid_config(polling_max_wait_hours=25)

    def test_invalid_batch_size(self):
        """import_batch_size hors limites doit lever une erreur"""
        with pytest.raises(ValueError, match="import_batch_size"):
            self._create_valid_config(import_batch_size=50)

        with pytest.raises(ValueError, match="import_batch_size"):
            self._create_valid_config(import_batch_size=50001)

    def test_invalid_smtp_port(self):
        """smtp_port hors limites doit lever une erreur"""
        with pytest.raises(ValueError, match="smtp_port"):
            self._create_valid_config(smtp_port=0)

        with pytest.raises(ValueError, match="smtp_port"):
            self._create_valid_config(smtp_port=65536)

    def test_invalid_smtp_from(self):
        """smtp_from invalide doit lever une erreur"""
        with pytest.raises(ValueError, match="smtp_from"):
            self._create_valid_config(smtp_from='invalid-email')

        with pytest.raises(ValueError, match="smtp_from"):
            self._create_valid_config(smtp_from='')

