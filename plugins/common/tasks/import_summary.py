"""Agrégation standardisée des résultats d'import (AMUE et ECC)."""
from typing import Dict, List


def summarize_import_results(import_results: List[Dict]) -> Dict[str, int]:
    """
    Agrège les compteurs d'une liste de résultats d'import.

    Le format de résultat par table est commun à AMUE et ECC :
        {table_name, rows_fetched, rows_inserted, rows_updated,
         rows_skipped, status, ...}

    Args:
        import_results: Liste de dicts de résultats par table

    Returns:
        Dict agrégé : tables_processed, tables_success, total_fetched,
        total_inserted, total_updated, total_skipped
    """
    return {
        'tables_processed': len(import_results),
        'tables_success': sum(1 for r in import_results if r.get('status') == 'success'),
        'total_fetched': sum(r.get('rows_fetched', 0) for r in import_results),
        'total_inserted': sum(r.get('rows_inserted', 0) for r in import_results),
        'total_updated': sum(r.get('rows_updated', 0) for r in import_results),
        'total_skipped': sum(r.get('rows_skipped', 0) for r in import_results),
    }
