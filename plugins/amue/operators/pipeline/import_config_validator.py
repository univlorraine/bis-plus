"""
Validation de la configuration d'import pour AMUEDataImporter.

Récupère et valide les clés primaires depuis splus_admin.amue_tables.
"""
import logging
from typing import List

from amue.services.table_config_manager import TableConfigManager

logger = logging.getLogger(__name__)


class ImportConfigValidator:
    """
    Récupération et validation des clés primaires pour l'import.

    Consulte splus_admin.amue_tables (via TableConfigManager) pour
    obtenir les PKs configurées par l'administrateur.

    Example:
        >>> validator = ImportConfigValidator()
        >>> pks = validator.get_primary_keys('CSKS')
        >>> # ['bukrs', 'kostl']
    """

    def get_primary_keys(self, table_name: str) -> List[str]:
        """
        Récupère les clés primaires depuis splus_admin.amue_tables.

        Args:
            table_name: Nom de la table (insensible à la casse)

        Returns:
            Liste des colonnes PK en minuscules, ou [] si absente/erreur
        """
        try:
            table = TableConfigManager().get_table_metadata(table_name)
            if table is None:
                return []
            pk_str = table.get('primary_key', '')
            if pk_str:
                return [pk.strip().lower() for pk in pk_str.split(',') if pk.strip()]
            return []
        except Exception as e:
            logger.warning(f"Erreur lecture PKs depuis config pour {table_name}: {e}")
            return []
