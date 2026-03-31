"""
Tests unitaires pour le service de retry intelligent
"""
import pytest
from unittest.mock import Mock, patch
import requests

from common.services.retry_service import (
    RetryService,
    RetryConfig,
    RetryStrategy,
    RetryResult,
    ErrorCategory,
    get_retry_service,
    reset_retry_service,
)


class TestErrorCategory:
    """Tests pour la catégorisation des erreurs"""

    def test_categorize_timeout(self):
        """Timeout doit être catégorisé comme TIMEOUT"""
        service = RetryService()
        error = requests.exceptions.Timeout("Connection timed out")
        assert service.categorize_error(error) == ErrorCategory.TIMEOUT

    def test_categorize_connection_error(self):
        """Erreur de connexion doit être catégorisée comme CONNECTION"""
        service = RetryService()
        error = requests.exceptions.ConnectionError("Failed to connect")
        assert service.categorize_error(error) == ErrorCategory.CONNECTION

    def test_categorize_rate_limit_429(self):
        """Code 429 doit être catégorisé comme RATE_LIMITED"""
        service = RetryService()

        response = Mock()
        response.status_code = 429
        error = requests.exceptions.HTTPError(response=response)

        assert service.categorize_error(error) == ErrorCategory.RATE_LIMITED

    def test_categorize_client_error_4xx(self):
        """Codes 4xx (sauf 429) doivent être catégorisés comme CLIENT_ERROR"""
        service = RetryService()

        for status_code in [400, 401, 403, 404, 422]:
            response = Mock()
            response.status_code = status_code
            error = requests.exceptions.HTTPError(response=response)

            assert service.categorize_error(error) == ErrorCategory.CLIENT_ERROR, \
                f"Code {status_code} devrait être CLIENT_ERROR"

    def test_categorize_server_error_5xx(self):
        """Codes 5xx doivent être catégorisés comme SERVER_ERROR"""
        service = RetryService()

        for status_code in [500, 502, 503, 504]:
            response = Mock()
            response.status_code = status_code
            error = requests.exceptions.HTTPError(response=response)

            assert service.categorize_error(error) == ErrorCategory.SERVER_ERROR, \
                f"Code {status_code} devrait être SERVER_ERROR"

    def test_categorize_unknown_error(self):
        """Erreurs inconnues doivent être catégorisées comme UNKNOWN"""
        service = RetryService()
        error = ValueError("Some random error")
        assert service.categorize_error(error) == ErrorCategory.UNKNOWN

    def test_categorize_status_code_directly(self):
        """Test de la catégorisation directe par code HTTP"""
        service = RetryService()

        assert service.categorize_status_code(429) == ErrorCategory.RATE_LIMITED
        assert service.categorize_status_code(400) == ErrorCategory.CLIENT_ERROR
        assert service.categorize_status_code(500) == ErrorCategory.SERVER_ERROR
        assert service.categorize_status_code(200) == ErrorCategory.UNKNOWN


class TestRetryStrategy:
    """Tests pour les stratégies de retry"""

    def test_calculate_delay_fixed(self):
        """Délai fixe sans backoff exponentiel"""
        strategy = RetryStrategy(
            max_retries=3,
            base_delay=5.0,
            max_delay=60.0,
            exponential=False,
            jitter=False
        )

        assert strategy.calculate_delay(0) == 5.0
        assert strategy.calculate_delay(1) == 5.0
        assert strategy.calculate_delay(5) == 5.0

    def test_calculate_delay_exponential(self):
        """Délai avec backoff exponentiel"""
        strategy = RetryStrategy(
            max_retries=5,
            base_delay=2.0,
            max_delay=60.0,
            exponential=True,
            jitter=False
        )

        assert strategy.calculate_delay(0) == 2.0    # 2 * 2^0 = 2
        assert strategy.calculate_delay(1) == 4.0    # 2 * 2^1 = 4
        assert strategy.calculate_delay(2) == 8.0    # 2 * 2^2 = 8
        assert strategy.calculate_delay(3) == 16.0   # 2 * 2^3 = 16

    def test_calculate_delay_max_cap(self):
        """Le délai doit être plafonné au max_delay"""
        strategy = RetryStrategy(
            max_retries=10,
            base_delay=10.0,
            max_delay=30.0,
            exponential=True,
            jitter=False
        )

        # 10 * 2^3 = 80, mais plafonné à 30
        assert strategy.calculate_delay(3) == 30.0
        assert strategy.calculate_delay(10) == 30.0

    def test_calculate_delay_with_jitter(self):
        """Le jitter ajoute une variation de ±20%"""
        strategy = RetryStrategy(
            max_retries=3,
            base_delay=10.0,
            max_delay=60.0,
            exponential=False,
            jitter=True
        )

        # Exécute plusieurs fois pour vérifier la variation
        delays = [strategy.calculate_delay(0) for _ in range(100)]

        # Tous les délais doivent être dans la plage [8, 12] (±20% de 10)
        assert all(8.0 <= d <= 12.0 for d in delays)

        # Il devrait y avoir de la variation
        assert len(set(delays)) > 1


class TestRetryConfig:
    """Tests pour la configuration des stratégies"""

    def test_default_strategies(self):
        """Les stratégies par défaut sont créées"""
        config = RetryConfig()

        # Vérifie que toutes les catégories ont une stratégie
        for category in ErrorCategory:
            assert category in config.strategies

    def test_client_error_no_retry(self):
        """Les erreurs client n'ont pas de retry"""
        config = RetryConfig()
        strategy = config.get_strategy(ErrorCategory.CLIENT_ERROR)

        assert strategy.max_retries == 0

    def test_rate_limit_aggressive_retry(self):
        """Les rate limits ont un retry agressif"""
        config = RetryConfig()
        strategy = config.get_strategy(ErrorCategory.RATE_LIMITED)

        assert strategy.max_retries >= 3
        assert strategy.jitter is True  # Jitter pour éviter synchronisation

    def test_custom_strategy(self):
        """Les stratégies personnalisées sont utilisées"""
        custom_strategy = RetryStrategy(
            max_retries=10,
            base_delay=1.0,
            max_delay=10.0,
            exponential=False,
            jitter=False
        )
        config = RetryConfig(strategies={
            ErrorCategory.SERVER_ERROR: custom_strategy
        })

        strategy = config.get_strategy(ErrorCategory.SERVER_ERROR)
        assert strategy.max_retries == 10
        assert strategy.base_delay == 1.0


class TestRetryService:
    """Tests pour le service de retry"""

    def test_should_retry_client_error(self):
        """Les erreurs client ne doivent pas être retryées"""
        service = RetryService()

        response = Mock()
        response.status_code = 400
        error = requests.exceptions.HTTPError(response=response)

        should_retry, delay = service.should_retry(error, attempt=0)
        assert should_retry is False

    def test_should_retry_server_error(self):
        """Les erreurs serveur doivent être retryées"""
        service = RetryService()

        response = Mock()
        response.status_code = 500
        error = requests.exceptions.HTTPError(response=response)

        should_retry, delay = service.should_retry(error, attempt=0)
        assert should_retry is True
        assert delay > 0

    def test_should_retry_max_attempts(self):
        """Pas de retry après max_retries tentatives"""
        service = RetryService()

        response = Mock()
        response.status_code = 500
        error = requests.exceptions.HTTPError(response=response)

        # La stratégie par défaut pour SERVER_ERROR a 3 retries
        should_retry, _ = service.should_retry(error, attempt=3)
        assert should_retry is False

    def test_execute_success_first_try(self):
        """Succès au premier essai"""
        service = RetryService()

        def operation():
            return "success"

        result = service.execute_with_retry(operation)

        assert result.success is True
        assert result.result == "success"
        assert result.attempts == 1
        assert result.total_delay == 0.0

    def test_execute_success_after_retry(self):
        """Succès après un retry"""
        service = RetryService()
        call_count = 0

        def operation():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                response = Mock()
                response.status_code = 500
                raise requests.exceptions.HTTPError(response=response)
            return "success"

        with patch('time.sleep'):  # Skip actual sleep
            result = service.execute_with_retry(operation)

        assert result.success is True
        assert result.result == "success"
        assert result.attempts == 2

    def test_execute_failure_no_retry_client_error(self):
        """Échec immédiat pour erreur client (pas de retry)"""
        service = RetryService()

        def operation():
            response = Mock()
            response.status_code = 400
            raise requests.exceptions.HTTPError(response=response)

        result = service.execute_with_retry(operation)

        assert result.success is False
        assert result.attempts == 1
        assert result.error_category == ErrorCategory.CLIENT_ERROR

    def test_execute_failure_after_max_retries(self):
        """Échec après épuisement des retries"""
        service = RetryService()

        def operation():
            response = Mock()
            response.status_code = 500
            raise requests.exceptions.HTTPError(response=response)

        with patch('time.sleep'):
            result = service.execute_with_retry(operation)

        assert result.success is False
        assert result.attempts == 4  # 1 initial + 3 retries
        assert result.error_category == ErrorCategory.SERVER_ERROR

    def test_execute_with_callback(self):
        """Le callback on_retry est appelé"""
        service = RetryService()
        callback_calls = []

        def operation():
            if len(callback_calls) < 1:
                response = Mock()
                response.status_code = 500
                raise requests.exceptions.HTTPError(response=response)
            return "success"

        def on_retry(attempt, error, delay):
            callback_calls.append((attempt, type(error).__name__, delay))

        with patch('time.sleep'):
            result = service.execute_with_retry(operation, on_retry)

        assert result.success is True
        assert len(callback_calls) == 1
        assert callback_calls[0][0] == 0  # Premier retry (attempt=0)

    def test_get_retry_info(self):
        """get_retry_info retourne les bonnes informations"""
        service = RetryService()

        response = Mock()
        response.status_code = 429
        error = requests.exceptions.HTTPError(response=response)

        info = service.get_retry_info(error)

        assert info['category'] == 'rate_limited'
        assert info['max_retries'] >= 3
        assert 'recommendation' in info


class TestRetryResult:
    """Tests pour RetryResult"""

    def test_bool_success(self):
        """RetryResult True si success"""
        result = RetryResult(success=True, result="data")
        assert bool(result) is True

    def test_bool_failure(self):
        """RetryResult False si échec"""
        result = RetryResult(success=False, error=Exception("error"))
        assert bool(result) is False


class TestSingleton:
    """Tests pour le singleton"""

    def test_get_retry_service_singleton(self):
        """get_retry_service retourne toujours la même instance"""
        reset_retry_service()

        service1 = get_retry_service()
        service2 = get_retry_service()

        assert service1 is service2

    def test_reset_retry_service(self):
        """reset_retry_service crée une nouvelle instance"""
        service1 = get_retry_service()
        reset_retry_service()
        service2 = get_retry_service()

        assert service1 is not service2
