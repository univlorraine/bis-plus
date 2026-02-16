"""
Gestionnaire d'import des donnees depuis l'API AMUE vers PostgreSQL.

================================================================================
ARCHITECTURE DU MODULE
================================================================================

Ce module gere l'import des donnees financieres depuis l'API AMUE vers une base
PostgreSQL. Il utilise une approche STREAMING pour optimiser la memoire lors du
traitement de gros volumes de donnees.

FLUX DE DONNEES :
    API AMUE  ->  Streaming (generateur)  ->  Batching  ->  PostgreSQL
       |              |                       |            |
   Pagination    Yield ligne/ligne      5000 lignes   INSERT/UPSERT

Ce module orchestre les sous-modules:
    - data_streamer.py  : Pagination et streaming depuis l'API
    - batch_inserter.py : Execution SQL par batch
    - duplicate_detector.py : Detection des doublons de PK

MODES D'IMPORT :
    1. FULL (complet) :
       - TRUNCATE + INSERT dans une transaction atomique
       - Rollback complet en cas d'erreur -> donnees originales preservees

    2. DIFFERENTIAL (differentiel) :
       - UPSERT (INSERT ON CONFLICT UPDATE) idempotent
       - Commit par batch (plus resilient aux erreurs)

================================================================================
CONFIGURATION
================================================================================

Variables Airflow utilisees :
    - universite              : Code universite (obligatoire)
    - api_endpoint_table      : Template URL avec $univ et $table (obligatoire)
    - amue_api_max_retries    : Nombre de tentatives API (defaut: 3)
    - amue_api_retry_delay_seconds : Delai entre tentatives (defaut: 30)
    - amue_import_batch_size  : Taille des batchs d'insertion (defaut: 5000)

================================================================================
"""
import json
import logging
import queue
import threading
from concurrent.futures import ThreadPoolExecutor, Future
from datetime import datetime
from string import Template
from typing import Dict, List, Optional, Tuple, Any

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
from amue.utils.config.airflow_helpers import AirflowVariableManager as VarMgr
from amue.utils.database.hooks import create_postgres_hook
from amue.utils.tracing import generate_correlation_id, TracingContext
from amue.types_amue import ImportResult, ImportConfig

logger = logging.getLogger(__name__)


class AMUEDataImporter:
    """
    Orchestre l'import des donnees depuis l'API AMUE vers PostgreSQL.

    Cette classe coordonne le streaming des donnees (AMUEDataStreamer)
    et leur insertion par batch (AMUEBatchInserter).

    Strategies d'import supportees :
        - INSERT simple : Pour les tables sans cle primaire ou import FULL
        - UPSERT : INSERT ON CONFLICT DO UPDATE pour les imports differentiels

    Attributes:
        api_hook: Hook de connexion a l'API AMUE (OAuth)
        postgres_hook: Hook de connexion PostgreSQL
        streamer: Service de streaming des donnees
        inserter: Service d'insertion par batch
        batch_size: Nombre de lignes par batch d'insertion

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

    # Taille de batch par defaut pour l'insertion
    DEFAULT_BATCH_SIZE = 5000

    # Cache des colonnes texte par (schema, table)
    _text_columns_cache: Dict[tuple, set] = {}

    def __init__(self, api_hook: Any, postgres_hook: Optional[PostgresHook] = None, target_schema: Optional[str] = None):
        """
        Initialise l'importeur de donnees AMUE.

        Args:
            api_hook: Instance de AMUEAPIHook pour les appels API
            postgres_hook: Hook PostgreSQL optionnel (cree si non fourni)
            target_schema: Schéma cible pour blue/green (ex: 'splus_blue')
                          Si None, utilise le schéma par défaut 'splus'

        Raises:
            AirflowException: Si les variables obligatoires sont manquantes
        """
        self.api_hook = api_hook
        self.target_schema = target_schema

        # Connexion PostgreSQL avec schema cible ou splus par defaut
        if postgres_hook:
            self.postgres_hook = postgres_hook
        elif target_schema:
            self.postgres_hook = create_postgres_hook(bluegreen_schema=target_schema)
        else:
            self.postgres_hook = PostgresHook(
                postgres_conn_id='postgres_data',
                options='-c search_path=splus'
            )

        # Chargement des variables obligatoires
        try:
            univ = VarMgr.get('universite')
        except KeyError:
            raise AirflowException("La variable 'universite' doit etre definie")

        try:
            endpointtbl = VarMgr.get('api_endpoint_table')
        except KeyError:
            raise AirflowException("La variable 'api_endpoint_table' doit etre definie")

        # Substitution du placeholder $univ dans l'endpoint
        try:
            self.endpoint = Template(endpointtbl).substitute(univ=univ)
        except KeyError as e:
            raise AirflowException(f"Erreur lors de la substitution dans l'endpoint: {e}")

        # Parametres de configuration
        self.max_retries = int(VarMgr.get('amue_api_max_retries', default='3'))
        self.retry_delay = int(VarMgr.get('amue_api_retry_delay_seconds', default='30'))
        self.batch_size = int(VarMgr.get('amue_import_batch_size', default=str(self.DEFAULT_BATCH_SIZE)))

        # Nombre de workers parallèles (1 = séquentiel)
        self.parallel_workers = int(VarMgr.get('amue_import_parallel_workers', default='1'))

        # Initialisation des sous-services avec le schéma cible
        self.streamer = AMUEDataStreamer(api_hook, self.endpoint)
        self.inserter = AMUEBatchInserter(self.postgres_hook, target_schema=target_schema)

        # Source par défaut pour les meta colonnes
        self.default_source = 'sifac_plus'

        if target_schema:
            logger.info(f"[IMPORT] Schéma cible blue/green: {target_schema}")

    def _get_primary_keys_from_config(self, table_name: str) -> List[str]:  # noqa: C901
        """
        Récupère les clés primaires depuis la variable Airflow.

        Args:
            table_name: Nom de la table

        Returns:
            Liste des colonnes formant la clé primaire
        """
        try:
            tables_var = VarMgr.get('amue_tables_to_import', default='[]')
            tables_config = json.loads(tables_var) if isinstance(tables_var, str) else tables_var

            for table in tables_config:
                if table.get('name', '').upper() == table_name.upper():
                    pk_str = table.get('primary_key', '')
                    if pk_str:
                        return [pk.strip().lower() for pk in pk_str.split(',') if pk.strip()]
            return []
        except Exception as e:
            logger.warning(f"Erreur lecture PKs depuis config pour {table_name}: {e}")
            return []

    @classmethod
    def clear_text_columns_cache(cls) -> None:
        """Vide le cache des colonnes texte (utile pour les tests)."""
        cls._text_columns_cache.clear()

    def _get_text_columns(self, table_name: str, columns: List[str]) -> set:
        """
        Récupère les colonnes de type texte depuis information_schema.

        Utilise un cache par (schema, table) pour éviter les requêtes
        répétées à information_schema.

        Les colonnes texte (character, character varying, text) acceptent
        les chaînes vides comme valeurs valides. Les autres types (numeric,
        integer, bytea, timestamp...) doivent recevoir NULL au lieu de ''.

        Args:
            table_name: Nom de la table
            columns: Liste des colonnes à vérifier

        Returns:
            Set des noms de colonnes qui sont de type texte
        """
        schema = self.target_schema or 'splus'
        cache_key = (schema, table_name.lower())

        if cache_key in self._text_columns_cache:
            logger.info(f"[IMPORT] {table_name}: colonnes texte depuis le cache")
            return self._text_columns_cache[cache_key]

        try:
            sql = """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s
                  AND table_name = %s
                  AND data_type IN ('character', 'character varying', 'text')
            """
            rows = self.postgres_hook.get_records(sql, parameters=(schema, table_name.lower()))
            text_cols = {row[0] for row in rows} if rows else set()
            logger.info(f"[IMPORT] {table_name}: {len(text_cols)} colonnes texte sur {len(columns)}")
            self._text_columns_cache[cache_key] = text_cols
            return text_cols
        except Exception as e:
            logger.warning(f"[IMPORT] Impossible de récupérer les types de colonnes pour {table_name}: {e}")
            # En cas d'erreur, on considère toutes les colonnes comme texte
            # pour ne pas casser l'import (pas de conversion '' -> NULL)
            return set(columns)

    @staticmethod
    def _queue_put_safe(batch_queue, item, error_event, timeout=1.0):
        """
        Put avec timeout et verification error_event pour eviter le deadlock.

        Si un consumer meurt et la queue est pleine, le producteur detecte
        l'erreur via error_event au lieu de bloquer indefiniment.

        Args:
            batch_queue: Queue dans laquelle inserer l'item
            item: Element a inserer (batch ou sentinelle None)
            error_event: threading.Event signalant une erreur consumer
            timeout: Timeout du put en secondes

        Raises:
            AMUEDatabaseError: Si un worker d'insertion a rencontre une erreur
        """
        while True:
            if error_event.is_set():
                raise AMUEDatabaseError("Un worker d'insertion a rencontre une erreur")
            try:
                batch_queue.put(item, timeout=timeout)
                return
            except queue.Full:
                continue

    def import_table(
        self,
        table_name: str,
        columns: List[str],
        primary_keys: List[str],
        import_config: Dict[str, Any]
    ) -> ImportResult:
        """
        Importe les donnees d'une table depuis l'API vers PostgreSQL.

        Point d'entree principal pour l'import d'une table.
        Coordonne le streaming des donnees et leur insertion par batch.

        Args:
            table_name: Nom de la table PostgreSQL cible
            columns: Liste des colonnes a importer (noms en minuscules)
            primary_keys: Liste des colonnes formant la cle primaire
            import_config: Configuration d'import contenant :
                - import_type: "full" ou "differential"
                - delta: Nom de la colonne de date pour import differentiel
                - last_import: Date ISO du dernier import
                - fingerprint_API: Empreinte de structure originale API
                - fingerprint_UL: Empreinte de structure transformée PG

        Returns:
            Dictionnaire avec le resultat de l'import :
            {
                "table_name": "CSKS",
                "rows_inserted": 1500,
                "rows_fetched": 1500,
                "import_type": "full",
                "fingerprint_API": "abc123...",
                "fingerprint_UL": "def456...",
                "status": "success"
            }

        Raises:
            AMUEImportError: Si l'import echoue (erreur API, DB, doublons...)
            AMUEDataError: Si les données sont invalides (doublons, PKs manquantes)
        """
        correlation_id = generate_correlation_id("import")
        logger.info(f"[{correlation_id}] Table: {table_name}, type: {import_config.get('import_type', 'full')}")

        try:
            with TracingContext(f"import_{table_name}") as ctx:
                # Récupère les PKs depuis la variable Airflow (prioritaire sur paramètre)
                config_pks = self._get_primary_keys_from_config(table_name)
                if config_pks:
                    primary_keys = config_pks
                    logger.info(f"[{correlation_id}] PKs depuis config Airflow: {primary_keys}")
                elif primary_keys:
                    logger.info(f"[{correlation_id}] PKs depuis paramètre: {primary_keys}")

                # UPSERT obligatoire - vérifie la présence de PKs
                if not primary_keys:
                    raise AMUEDataError(
                        f"Table {table_name.upper()} sans primary_key définie - UPSERT impossible. "
                        f"Configurez primary_key dans amue_tables_to_import ou vérifiez l'API.",
                        table_name=table_name,
                        correlation_id=correlation_id
                    )

                import_type = import_config.get('import_type', 'full')
                # UPSERT toujours activé (plus de TRUNCATE)
                use_upsert = True
                logger.info(f"[{correlation_id}] Mode UPSERT forcé pour {table_name}")

                # Ajoute les meta colonnes à la liste des colonnes
                columns_with_meta = list(columns) + ['_source', '_imported_at']

                # Lance le streaming et l'insertion par batch
                rows_inserted, rows_fetched, batch_metrics = self._stream_and_insert(
                    table_name,
                    columns_with_meta,
                    primary_keys,
                    import_config,
                    use_upsert,
                    correlation_id
                )

                # Ajoute les métriques au contexte de tracing
                ctx.add_metadata('rows_inserted', rows_inserted)
                ctx.add_metadata('rows_fetched', rows_fetched)

                # Calcul des métriques enrichies
                batch_count = len(batch_metrics)
                total_duration = sum(m.get('duration_seconds', 0) for m in batch_metrics)
                avg_batch_duration = round(total_duration / batch_count, 3) if batch_count > 0 else 0

                return {
                    'table_name': table_name,
                    'rows_inserted': rows_inserted,
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
            # Les exceptions AMUE sont propagées telles quelles
            raise
        except AirflowException:
            # Les AirflowException sont propagées telles quelles
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
    ) -> Tuple[int, int, List[Dict]]:
        """
        Orchestre le streaming des donnees et leur insertion par batch.

        Pattern producteur/consommateur avec queue.Queue :
            - Thread principal (producteur) : stream l'API et accumule les batches
            - Thread(s) consommateur(s) : inserent les batches en base avec COMMIT
            - Backpressure via maxsize=num_workers+1

        Meme avec 1 seul worker, le recouvrement API/DB est effectif :
        pendant que PG insere le batch N, l'API stream deja le batch N+1.

        Args:
            table_name: Nom de la table cible
            columns: Liste des colonnes (incluant _source et _imported_at)
            primary_keys: Cles primaires pour UPSERT
            import_config: Configuration d'import
            use_upsert: Toujours True (conservé pour compatibilité)
            correlation_id: ID de corrélation pour le tracing

        Returns:
            Tuple (rows_inserted, rows_fetched, batch_metrics)

        Raises:
            AMUEDatabaseError: En cas d'erreur de connexion DB
            AMUEBatchError: En cas d'erreur d'insertion batch
            AMUEDataError: En cas de données invalides
        """
        num_workers = self.parallel_workers
        data_columns = [c for c in columns if c not in ('_source', '_imported_at')]
        text_columns = self._get_text_columns(table_name, data_columns)
        import_timestamp = datetime.now()

        # Setup des workers
        if num_workers > 1:
            logger.info(f"[{correlation_id}] Mode pipeline: {num_workers} workers pour {table_name}")
            own_workers = True
            worker_inserters: List[AMUEBatchInserter] = []
            for _ in range(num_workers):
                worker_hook = create_postgres_hook(bluegreen_schema=self.target_schema) \
                    if self.target_schema else PostgresHook(
                        postgres_conn_id='postgres_data',
                        options='-c search_path=splus'
                    )
                worker_inserters.append(AMUEBatchInserter(worker_hook, target_schema=self.target_schema))
        else:
            own_workers = False
            worker_inserters = [self.inserter]

        # Build SQL par worker (chaque worker a sa propre connexion)
        insert_sqls: List[str] = []
        for w in worker_inserters:
            conn = w.get_connection()
            insert_sqls.append(
                w.build_insert_sql_for_values(table_name, columns, primary_keys, use_upsert, conn)
            )

        # Queue avec backpressure et event d'erreur
        batch_queue: queue.Queue = queue.Queue(maxsize=num_workers + 1)
        error_event = threading.Event()

        # Metriques partagees (protegees par lock)
        batch_metrics: List[Dict] = []
        metrics_lock = threading.Lock()

        def consumer(worker_idx: int) -> None:
            """Fonction consommateur executee dans un thread."""
            w = worker_inserters[worker_idx]
            conn = w.get_connection()
            cursor = conn.cursor()
            try:
                while True:
                    if error_event.is_set():
                        break
                    try:
                        item = batch_queue.get(timeout=1.0)
                    except queue.Empty:
                        continue
                    if item is None:
                        break
                    batch_data, b_num = item
                    metrics = w.execute_batch(
                        cursor, conn, insert_sqls[worker_idx], batch_data,
                        table_name, columns, primary_keys,
                        commit=True, batch_num=b_num
                    )
                    if metrics:
                        with metrics_lock:
                            batch_metrics.append(metrics)
            except Exception:
                error_event.set()
                raise
            finally:
                cursor.close()

        total_inserted = 0
        total_fetched = 0
        batch: List[tuple] = []
        batch_num = 0

        try:
            with ThreadPoolExecutor(max_workers=num_workers) as executor:
                # Lance les consumers
                futures: List[Future] = []
                for i in range(num_workers):
                    futures.append(executor.submit(consumer, i))

                # Producteur : stream et accumule les batches
                producer_error = None
                try:
                    for row in self.streamer.stream_data(table_name, import_config):
                        total_fetched += 1

                        row_lower = {k.lower(): v for k, v in row.items()} if isinstance(row, dict) else {}
                        data_values = [
                            None if row_lower.get(col, None) == '' and col not in text_columns
                            else row_lower.get(col, None)
                            for col in data_columns
                        ]
                        meta_values = [self.default_source, import_timestamp]
                        record = tuple(data_values + meta_values)
                        batch.append(record)

                        if len(batch) >= self.batch_size:
                            batch_num += 1
                            batch_copy = list(batch)
                            self._queue_put_safe(batch_queue, (batch_copy, batch_num), error_event)
                            total_inserted += len(batch)
                            logger.info(f"{table_name}: {total_inserted:,} lignes inserees (UPSERT)")
                            batch.clear()

                    # Dernier batch partiel
                    if batch:
                        batch_num += 1
                        batch_copy = list(batch)
                        self._queue_put_safe(batch_queue, (batch_copy, batch_num), error_event)
                        total_inserted += len(batch)

                    # Envoie les sentinelles (une par consumer)
                    for _ in range(num_workers):
                        self._queue_put_safe(batch_queue, None, error_event)
                except Exception as e:
                    # Signal aux consumers de s'arreter pour eviter le deadlock
                    error_event.set()
                    producer_error = e

                # Attend la fin des consumers et propage les erreurs
                # (les erreurs consumer ont priorite car elles sont la cause racine)
                for f in futures:
                    f.result()

                # Si le producteur a eu une erreur mais les consumers sont OK
                if producer_error is not None:
                    raise producer_error

            logger.info(f"[{correlation_id}] {table_name}: Total {total_inserted:,}/{total_fetched:,} lignes (UPSERT)")
            return total_inserted, total_fetched, batch_metrics

        except (AMUEDatabaseError, AMUEBatchError, AMUEDataError):
            raise
        except AirflowException:
            raise
        except Exception as e:
            logger.error(f"[{correlation_id}] Erreur insertion {table_name}: {e}")
            raise AMUEDatabaseError(
                f"Erreur d'insertion pour {table_name}: {e}",
                table_name=table_name,
                correlation_id=correlation_id
            ) from e
        finally:
            if own_workers:
                for w in worker_inserters:
                    w.close_connection()
