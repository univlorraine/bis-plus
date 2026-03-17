# tests/services/api/test_polling_strategy_calculator.py
"""Tests unitaires pour PollingStrategyCalculator."""
from unittest.mock import MagicMock


def make_config(interval_minutes=10, max_wait_hours=6,
                exponential_backoff=False, max_backoff_minutes=60):
    cfg = MagicMock()
    cfg.interval_minutes = interval_minutes
    cfg.max_wait_hours = max_wait_hours
    cfg.exponential_backoff = exponential_backoff
    cfg.max_backoff_minutes = max_backoff_minutes
    return cfg


class TestMaxAttempts:
    """Tests pour max_attempts()."""

    def test_standard_config(self):
        """6h / 10min = 36 tentatives."""
        from amue.services.api.polling_strategy_calculator import PollingStrategyCalculator
        calc = PollingStrategyCalculator(make_config(interval_minutes=10, max_wait_hours=6))
        assert calc.max_attempts() == 36

    def test_minimum_one(self):
        """Minimum 1 tentative même si l'intervalle > durée totale."""
        from amue.services.api.polling_strategy_calculator import PollingStrategyCalculator
        calc = PollingStrategyCalculator(make_config(interval_minutes=120, max_wait_hours=1))
        assert calc.max_attempts() >= 1

    def test_short_interval(self):
        """1h / 5min = 12 tentatives."""
        from amue.services.api.polling_strategy_calculator import PollingStrategyCalculator
        calc = PollingStrategyCalculator(make_config(interval_minutes=5, max_wait_hours=1))
        assert calc.max_attempts() == 12

    def test_large_window(self):
        """24h / 10min = 144 tentatives."""
        from amue.services.api.polling_strategy_calculator import PollingStrategyCalculator
        calc = PollingStrategyCalculator(make_config(interval_minutes=10, max_wait_hours=24))
        assert calc.max_attempts() == 144


class TestWaitTimeFixed:
    """Tests pour wait_time() avec stratégie fixe."""

    def test_always_returns_interval(self):
        """Sans backoff, retourne toujours l'intervalle."""
        from amue.services.api.polling_strategy_calculator import PollingStrategyCalculator
        calc = PollingStrategyCalculator(make_config(interval_minutes=10, exponential_backoff=False))
        assert calc.wait_time(1) == 10
        assert calc.wait_time(5) == 10
        assert calc.wait_time(100) == 10

    def test_different_intervals(self):
        """Différents intervalles sont respectés."""
        from amue.services.api.polling_strategy_calculator import PollingStrategyCalculator
        for interval in [5, 15, 30, 60]:
            calc = PollingStrategyCalculator(make_config(interval_minutes=interval, exponential_backoff=False))
            assert calc.wait_time(1) == interval
            assert calc.wait_time(3) == interval


class TestWaitTimeBackoff:
    """Tests pour wait_time() avec backoff exponentiel."""

    def test_first_attempt_equals_interval(self):
        """Tentative 1 : 2^0 × interval = interval."""
        from amue.services.api.polling_strategy_calculator import PollingStrategyCalculator
        calc = PollingStrategyCalculator(make_config(
            interval_minutes=10, exponential_backoff=True, max_backoff_minutes=60
        ))
        assert calc.wait_time(1) == 10.0

    def test_doubles_each_attempt(self):
        """Chaque tentative double le temps d'attente."""
        from amue.services.api.polling_strategy_calculator import PollingStrategyCalculator
        calc = PollingStrategyCalculator(make_config(
            interval_minutes=10, exponential_backoff=True, max_backoff_minutes=9999
        ))
        assert calc.wait_time(1) == 10.0
        assert calc.wait_time(2) == 20.0
        assert calc.wait_time(3) == 40.0
        assert calc.wait_time(4) == 80.0

    def test_capped_at_max_backoff(self):
        """Le temps d'attente est plafonné à max_backoff_minutes."""
        from amue.services.api.polling_strategy_calculator import PollingStrategyCalculator
        calc = PollingStrategyCalculator(make_config(
            interval_minutes=10, exponential_backoff=True, max_backoff_minutes=60
        ))
        # attempt=4 : 10 * 2^3 = 80 > 60 → 60
        assert calc.wait_time(4) == 60.0
        assert calc.wait_time(10) == 60.0

    def test_cap_not_exceeded(self):
        """Sans dépasser le cap, la valeur normale est retournée."""
        from amue.services.api.polling_strategy_calculator import PollingStrategyCalculator
        calc = PollingStrategyCalculator(make_config(
            interval_minutes=10, exponential_backoff=True, max_backoff_minutes=60
        ))
        # attempt=2 : 10 * 2 = 20 < 60
        assert calc.wait_time(2) == 20.0
