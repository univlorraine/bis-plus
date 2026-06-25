"""
Layer: domain

Types d'état partagés entre AdminStateManager et les services Blue/Green.

Placé directement sous `common.services` (et non `common.application.bluegreen`)
afin d'éviter que son import ne déclenche `common.application.bluegreen.__init__`,
qui importe `BlueGreenManager` -> `BlueGreenStateManager` -> `AdminStateManager`.
Cela permet à `admin_state_manager.py` et `bluegreen_state_manager.py` de
s'importer mutuellement au niveau module sans cycle.
"""
from dataclasses import dataclass
from typing import Dict


@dataclass(frozen=True)
class BlueGreenState:
    """État du déploiement blue/green (verrou d'import + audit)."""

    last_import_schema: str = ""
    last_switch_timestamp: str = ""
    last_sync_timestamp: str = ""
    import_in_progress: bool = False
    import_started_at: str = ""        # Timestamp ISO du début de l'import
    import_correlation_id: str = ""    # ID de corrélation de l'import en cours

    def to_dict(self) -> Dict:
        """Convertit l'état en dictionnaire."""
        return {
            "last_import_schema": self.last_import_schema,
            "last_switch_timestamp": self.last_switch_timestamp,
            "last_sync_timestamp": self.last_sync_timestamp,
            "import_in_progress": self.import_in_progress,
            "import_started_at": self.import_started_at,
            "import_correlation_id": self.import_correlation_id,
        }

    @classmethod
    def from_dict(cls, data: Dict) -> 'BlueGreenState':
        """Crée un état depuis un dictionnaire (ignore les champs inconnus)."""
        return cls(
            last_import_schema=data.get("last_import_schema", ""),
            last_switch_timestamp=data.get("last_switch_timestamp", ""),
            last_sync_timestamp=data.get("last_sync_timestamp", ""),
            import_in_progress=data.get("import_in_progress", False),
            import_started_at=data.get("import_started_at", ""),
            import_correlation_id=data.get("import_correlation_id", ""),
        )
