"""Layer: application

Fragments DDL partagés pour la création de tables d'import (AMUE/ECC) :
colonnes méta de traçabilité (_source, _imported_at) ajoutées par tous les
imports, indépendamment du schéma source.
"""
from typing import List


def build_meta_column_defs(default_source: str) -> List[str]:
    """Définitions DDL des colonnes méta ajoutées à toute table importée.

    Args:
        default_source: Valeur par défaut de `_source` (ex: 'sifac_plus', 'ecc').

    Returns:
        Liste de fragments DDL, ex: ["_source VARCHAR(50) DEFAULT 'ecc'",
        "_imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"].
    """
    return [
        f"_source VARCHAR(50) DEFAULT '{default_source}'",
        "_imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
    ]
