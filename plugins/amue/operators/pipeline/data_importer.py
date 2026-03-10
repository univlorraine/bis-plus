"""
Gestionnaire d'import des données depuis l'API AMUE vers PostgreSQL.

================================================================================
ARCHITECTURE DU MODULE
================================================================================

Ce module orchestre l'import des données financières depuis l'API AMUE vers
une base PostgreSQL. Il utilise une approche STREAMING pour optimiser la
mémoire lors du traitement de gros volumes de données.

FLUX DE DONNÉES :
    API AMUE  →  Streaming (générateur)  →  Batching  →  PostgreSQL
       |              |                       |            |
   Pagination    Yield ligne/ligne      5000 lignes   INSERT/UPSERT

================================================================================
ARCHITECTURE INTERNE (après refactorisation)
================================================================================

AMUEDataImporter compose :
    - ImportConfigValidator : récupération des PKs depuis splus_admin.amue_tables
    - DataImportPipeline    : pipeline producteur/consommateur threading

================================================================================
"""
import logging
from datetime import datetime
from string import Template
from typing import Dict, List, Optional, Any

from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from amue.exceptions import (
    AMUEImportError,
    AMUEDataError,
    AMUEDatabaseError,
    AMUEBatchError,
)
from amue.operators.pipeline.data_streamer import AMUEDataStreamer
from amue.operators.pipeline.batch_inserter import AMUEBatchInserter
from amue.operators.pipeline.import_config_validator import ImportConfigValidator
from amue.operators.pipeline.data_import_pipeline import DataImportPipeline
from amue.utils.config.airflow_helpers import AirflowVariableManager as VarMgr
from amue.utils.database.hooks import create_postgres_hook
from amue.utils.tracing import generate_correlation_id, TracingContext
from amue.types_amue import ImportResult, ImportConfig

logger = logging.getLogger(__name__)


class AMUEDataImporter:
    """
    Orchestre l'import des données depuis l'API AMUE vers PostgreSQL.

    Cette classe coordonne le streaming (AMUEDataStreamer) et l'insertion
    par batch (AMUEBatchInserter) via le DataImportPipeline.

    Délègue à :
        - ImportConfigValidator : récupération des clés primaires depuis la config
        - DataImportPipeline    : pipeline producteur/consommateur threading

    Example:
        >>> api_hook = AMUEAPIHook()
        >>> importer = AMUEDataImporter(api_hook)
        >>> result = importer.import_table(
        ...     table_name='CSKS',
        ...     columns=['bukrs', 'kostl', 'datab'],
        ...     primary_keys=['bukrs', 'kostl'],
        ...     import_config={'import_type': 'differential'}
        ... )
    """

    DEFAULT_BATCH_SIZE = 5000

    def __init__(self, api_hook: Any, postgres_hook: Optional[PostgresHook] = None,
                 target_schema: Optional[str] = None):
        """
        Args:
            api_hook: Instance de AMUEAPIHook pour les appels API
            postgres_hook: Hook PostgreSQL optionnel (créé si non fourni)
            target_schema: Schéma cible pour blue/green (ex: 'splus_blue')
        """
        self.api_hook = api_hook
        self.target_schema = target_schema

        if postgres_hook:
            self.postgres_hook = postgres_hook
        else:
            self.postgres_hook = create_postgres_hook(bluegreen_schema=target_schema)

        try:
            univ = VarMgr.get('universite')
        except KeyError:
            raise AirflowException("La variable 'universite' doit être définie")

        try:
            endpointtbl = VarMgr.get('api_endpoint_table')
        except KeyError:
            raise AirflowException("La variable 'api_endpoint_table' doit être définie")

        try:
            self.endpoint = Template(endpointtbl).substitute(univ=univ)
        except KeyError as e:
            raise AirflowException(f"Erreur lors de la substitution dans l'endpoint: {e}")

        self.max_retries = int(VarMgr.get('amue_api_max_retries', default='3'))
        self.retry_delay = int(VarMgr.get('amue_api_retry_delay_seconds', default='30'))
        self.batch_size = int(VarMgr.get('amue_import_batch_size', default=str(self.DEFAULT_BATCH_SIZE)))
        self.parallel_workers = int(VarMgr.get('amue_import_parallel_workers', default='1'))

        self.streamer = AMUEDataStreamer(api_hook, self.endpoint)
        self.inserter = AMUEBatchInserter(self.postgres_hook, target_schema=target_schema)

        # Sous-composants de la refactorisation
        self._config_validator = ImportConfigValidator()
        self._pipeline = DataImportPipeline(self.streamer, self.inserter, target_schema)

        if target_schema:
            logger.info(f"[IMPORT] Schéma cible blue/green: {target_schema}")

    def _get_primary_keys_from_config(self, table_name: str) -> List[str]:
        """Récupère les clés primaires depuis splus_admin.amue_tables."""
        return self._config_validator.get_primary_keys(table_name)

    @staticmethod
    def _queue_put_safe(batch_queue, item, error_event, timeout=1.0):
        """Délégué à DataImportPipeline._queue_put_safe (rétrocompatibilité)."""
        DataImportPipeline._queue_put_safe(batch_queue, item, error_event, timeout)

    def import_table(
        self,
        table_name: str,
        columns: List[str],
        primary_keys: List[str],
        import_config: Dict[str, Any]
    ) -> ImportResult:
        """
        Importe les données d'une table depuis l'API vers PostgreSQL.

        Args:
            table_name: Nom de la table PostgreSQL cible
            columns: Liste des colonnes à importer (noms en minuscules)
            primary_keys: Liste des colonnes formant la clé primaire
            import_config: Configuration d'import

        Returns:
            Dictionnaire avec le résultat de l'import

        Raises:
            AMUEImportError: Si l'import échoue
            AMUEDataError: Si les données sont invalides
        """
        correlation_id = generate_correlation_id("import")
        logger.info(f"[{correlation_id}] Table: {table_name}, type: {import_config.get('import_type', 'full')}")

        try:
            with TracingContext(f"import_{table_name}") as ctx:
                # Récupère les PKs depuis la config (prioritaire sur paramètre)
                config_pks = self._get_primary_keys_from_config(table_name)
                if config_pks:
                    primary_keys = config_pks
                    logger.info(f"[{correlation_id}] PKs depuis config Airflow: {primary_keys}")
                elif primary_keys:
                    logger.info(f"[{correlation_id}] PKs depuis paramètre: {primary_keys}")

                if not primary_keys:
                    raise AMUEDataError(
                        f"Table {table_name.upper()} sans primary_key définie - UPSERT impossible. "
                        f"Configurez primary_key dans amue_tables_to_import ou vérifiez l'API.",
                        table_name=table_name,
                        correlation_id=correlation_id
                    )

                import_type = import_config.get('import_type', 'full')
                logger.info(f"[{correlation_id}] Mode UPSERT forcé pour {table_name}")

                columns_with_meta = list(columns) + ['_source', '_imported_at']

                rows_inserted, rows_updated, rows_fetched, batch_metrics = self._stream_and_insert(
                    table_name, columns_with_meta, primary_keys,
                    import_config, True, correlation_id
                )

                ctx.add_metadata('rows_inserted', rows_inserted)
                ctx.add_metadata('rows_updated', rows_updated)
                ctx.add_metadata('rows_fetched', rows_fetched)

                batch_count = len(batch_metrics)
                total_duration = sum(m.get('duration_seconds', 0) for m in batch_metrics)
                avg_batch_duration = round(total_duration / batch_count, 3) if batch_count > 0 else 0

                return {
                    'table_name': table_name,
                    'rows_inserted': rows_inserted,
                    'rows_updated': rows_updated,
                    'rows_fetched': rows_fetched,
                    'import_type': import_type,
                    'fingerprint_API': import_config.get('fingerprint_API', ''),
                    'fingerprint_UL': import_config.get('fingerprint_UL', ''),
                    'table_finish': import_config.get('table_finish', ''),
                    'status': 'success',
                    'correlation_id': correlation_id,
                    'batch_count': batch_count,
                    'total_duration_seconds': round(total_duration, 3),
                    'avg_batch_duration': avg_batch_duration,
                }

        except (AMUEImportError, AMUEDataError, AMUEDatabaseError, AMUEBatchError):
            raise
        except AirflowException:
            raise
        except Exception as e:
            logger.error(f"[{correlation_id}] Erreur import {table_name}: {e}")
            raise AMUEImportError(
                f"Erreur inattendue lors de l'import de {table_name}: {e}",
                table_name=table_name,
                correlation_id=correlation_id
            ) from e
        finally:
            self.inserter.close_connection()

    def _stream_and_insert(
        self,
        table_name: str,
        columns: List[str],
        primary_keys: List[str],
        import_config: Dict[str, Any],
        use_upsert: bool,
        correlation_id: str = ""
    ):
        """
        Orchestre le pipeline producteur/consommateur.

        Crée les workers multi-thread ici (imports patchables par les tests),
        puis délègue à DataImportPipeline.run() pour la logique threading.
        """
        worker_inserters = None
        if self.parallel_workers > 1:
            worker_inserters = []
            for _ in range(self.parallel_workers):
                hook = create_postgres_hook(bluegreen_schema=self.target_schema)
                worker_inserters.append(
                    AMUEBatchInserter(hook, target_schema=self.target_schema)
                )

        return self._pipeline.run(
            table_name=table_name,
            columns=columns,
            primary_keys=primary_keys,
            import_config=import_config,
            batch_size=self.batch_size,
            num_workers=self.parallel_workers,
            correlation_id=correlation_id,
            worker_inserters=worker_inserters,
        )
