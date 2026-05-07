"""
Operators AMUE

Sous-packages :
    - pipeline : Import de données (streaming, batch, doublons)
    - table_management : Gestion des tables (filtrage, création, vérification)
"""
from amue.operators.pipeline import (
    AMUEDataImporter,
    AMUEDataStreamer,
    BatchInserter,
    DuplicateDetector,
)
from amue.operators.table_management import (
    AMUETableFilter,
    AMUETableManager,
    AMUETableVerifier,
)

__all__ = [
    'AMUEDataImporter',
    'AMUEDataStreamer',
    'BatchInserter',
    'DuplicateDetector',
    'AMUETableFilter',
    'AMUETableManager',
    'AMUETableVerifier',
]
