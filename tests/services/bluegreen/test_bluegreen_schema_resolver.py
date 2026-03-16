"""
Tests unitaires pour BlueGreenSchemaResolver.
"""
from unittest.mock import MagicMock


def _make_resolver(view_schema_result):
    """Crée un BlueGreenSchemaResolver avec un ViewSwitcher mocké."""
    from amue.services.bluegreen.bluegreen_schema_resolver import BlueGreenSchemaResolver

    mock_vs = MagicMock()
    mock_vs.get_current_target_schema.return_value = view_schema_result
    resolver = BlueGreenSchemaResolver(view_switcher=mock_vs)
    return resolver


class TestBlueGreenSchemaResolverGetTargetSchema:
    """Tests pour get_target_schema()."""

    def test_target_is_green_when_active_is_blue(self):
        """Retourne splus_green si les vues pointent vers splus_blue."""
        resolver = _make_resolver("splus_blue")
        assert resolver.get_target_schema() == "splus_green"

    def test_target_is_blue_when_active_is_green(self):
        """Retourne splus_blue si les vues pointent vers splus_green."""
        resolver = _make_resolver("splus_green")
        assert resolver.get_target_schema() == "splus_blue"

    def test_target_is_blue_when_no_views(self):
        """Retourne splus_blue si aucune vue n'existe (premier import)."""
        resolver = _make_resolver(None)
        assert resolver.get_target_schema() == "splus_blue"


class TestBlueGreenSchemaResolverGetActiveSchema:
    """Tests pour get_active_schema()."""

    def test_active_is_blue_when_views_point_to_blue(self):
        """Retourne splus_blue si les vues pointent vers splus_blue."""
        resolver = _make_resolver("splus_blue")
        assert resolver.get_active_schema() == "splus_blue"

    def test_active_is_green_when_views_point_to_green(self):
        """Retourne splus_green si les vues pointent vers splus_green."""
        resolver = _make_resolver("splus_green")
        assert resolver.get_active_schema() == "splus_green"

    def test_active_is_green_when_no_views(self):
        """Retourne splus_green si aucune vue n'existe (cohérent avec target=blue)."""
        resolver = _make_resolver(None)
        assert resolver.get_active_schema() == "splus_green"


class TestBlueGreenSchemaResolverGetInactiveSchema:
    """Tests pour get_inactive_schema()."""

    def test_inactive_is_green_when_active_is_blue(self):
        """Retourne splus_green si actif = splus_blue."""
        resolver = _make_resolver("splus_blue")
        assert resolver.get_inactive_schema() == "splus_green"

    def test_inactive_is_blue_when_active_is_green(self):
        """Retourne splus_blue si actif = splus_green."""
        resolver = _make_resolver("splus_green")
        assert resolver.get_inactive_schema() == "splus_blue"

    def test_inactive_is_blue_when_no_views(self):
        """Retourne splus_blue si aucune vue (actif par défaut = splus_green)."""
        resolver = _make_resolver(None)
        assert resolver.get_inactive_schema() == "splus_blue"


class TestBlueGreenSchemaResolverGetViewSchema:
    """Tests pour get_view_schema()."""

    def test_view_schema_is_splus(self):
        """Retourne toujours 'splus' quelle que soit la configuration."""
        resolver = _make_resolver("splus_blue")
        assert resolver.get_view_schema() == "splus"


class TestBlueGreenSchemaResolverConsistency:
    """Tests de cohérence entre active, inactive et target."""

    def test_active_and_target_are_complementary_blue(self):
        """active + target couvrent les deux schémas quand views → blue."""
        resolver = _make_resolver("splus_blue")
        active = resolver.get_active_schema()
        target = resolver.get_target_schema()

        assert active != target
        assert {active, target} == {"splus_blue", "splus_green"}

    def test_active_and_target_are_complementary_green(self):
        """active + target couvrent les deux schémas quand views → green."""
        resolver = _make_resolver("splus_green")
        active = resolver.get_active_schema()
        target = resolver.get_target_schema()

        assert active != target
        assert {active, target} == {"splus_blue", "splus_green"}

    def test_inactive_equals_target(self):
        """get_inactive_schema() retourne le même schéma que get_target_schema()."""
        resolver = _make_resolver("splus_blue")
        assert resolver.get_inactive_schema() == resolver.get_target_schema()

    def test_lazy_view_switcher_instantiated_on_first_access(self):
        """Le ViewSwitcher est créé lazily quand view_switcher=None."""
        from amue.services.bluegreen.bluegreen_schema_resolver import BlueGreenSchemaResolver

        resolver = BlueGreenSchemaResolver(view_switcher=None)
        assert resolver._view_switcher is None  # Pas encore instancié

        # On injecte un mock après construction pour éviter l'appel à ViewSwitcher réel
        mock_vs = MagicMock()
        mock_vs.get_current_target_schema.return_value = "splus_blue"
        resolver._view_switcher = mock_vs

        result = resolver.get_active_schema()
        assert result == "splus_blue"
