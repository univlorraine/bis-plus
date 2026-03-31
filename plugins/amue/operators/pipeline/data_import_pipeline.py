"""
Pipeline producteur/consommateur pour l'import de données AMUE.

Orchestre le streaming depuis l'API AMUE et l'insertion par batch
dans PostgreSQL via un pattern producteur/consommateur avec queue.Queue.

Même avec 1 seul worker, le recouvrement API/DB est effectif :
pendant que PG insère le batch N, l'API stream déjà le batch N+1.
"""
import logging
import queue
import threading
from concurrent.futures import ThreadPoolExecutor, Future
from datetime import datetime
from typing import Dict, List, Tuple, Any

from amue.exceptions import AMUEDatabaseError, AMUEBatchError, AMUEDataError
from common.operators.batch_inserter import AMUEBatchInserter
from common.utils.database.hooks import create_postgres_hook
from common.config import PROTECTED_SOURCE

logger = logging.getLogger(__name__)


class DataImportPipeline:
    """
    Pipeline producteur/consommateur pour l'import de données.

    Pattern :
        Thread principal (producteur) : stream l'API et accumule les batches
        Thread(s) consommateur(s)     : insèrent les batches en base avec COMMIT
        Backpressure via maxsize=num_workers+1

    Example:
        >>> pipeline = DataImportPipeline(streamer, inserter, target_schema)
        >>> inserted, updated, fetched, metrics = pipeline.run(
        ...     table_name='CSKS',
        ...     columns=['bukrs', 'kostl', '_source', '_imported_at'],
        ...     primary_keys=['bukrs', 'kostl'],
        ...     import_config={...},
        ...     batch_size=5000,
        ...     num_workers=1,
        ...     correlation_id='import-abc123',
        ... )
    """

    def __init__(self, streamer, inserter: AMUEBatchInserter, target_schema: str = None):
        """
        Args:
            streamer: AMUEDataStreamer pour la lecture depuis l'API
            inserter: AMUEBatchInserter pour l'insertion PostgreSQL
            target_schema: Schéma cible blue/green (ex: 'splus_green')
        """
        self.streamer = streamer
        self.inserter = inserter
        self.target_schema = target_schema

    @staticmethod
    def _queue_put_safe(batch_queue: queue.Queue, item: Any, error_event: threading.Event,
                        timeout: float = 1.0) -> None:
        """
        Put avec timeout et vérification error_event pour éviter le deadlock.

        Si un consumer meurt et la queue est pleine, le producteur détecte
        l'erreur via error_event au lieu de bloquer indéfiniment.

        Raises:
            AMUEDatabaseError: Si un worker d'insertion a rencontré une erreur
        """
        while True:
            if error_event.is_set():
                raise AMUEDatabaseError("Un worker d'insertion a rencontré une erreur")
            try:
                batch_queue.put(item, timeout=timeout)
                return
            except queue.Full:
                continue

    def run(
        self,
        table_name: str,
        columns: List[str],
        primary_keys: List[str],
        import_config: Dict[str, Any],
        batch_size: int,
        num_workers: int = 1,
        correlation_id: str = "",
        worker_inserters: List[AMUEBatchInserter] = None,
    ) -> Tuple[int, int, int, List[Dict]]:
        """
        Exécute le pipeline producteur/consommateur.

        Args:
            table_name: Nom de la table cible
            columns: Liste des colonnes (incluant _source et _imported_at)
            primary_keys: Clés primaires pour UPSERT
            import_config: Configuration d'import
            batch_size: Nombre de lignes par batch
            num_workers: Nombre de workers consommateurs
            correlation_id: ID de corrélation pour le tracing

        Returns:
            Tuple (rows_inserted, rows_updated, rows_fetched, batch_metrics)

        Raises:
            AMUEDatabaseError: En cas d'erreur de connexion DB
            AMUEBatchError: En cas d'erreur d'insertion batch
            AMUEDataError: En cas de données invalides
        """
        data_columns = [c for c in columns if c not in ('_source', '_imported_at')]
        import_timestamp = datetime.now()

        # Setup des workers
        if worker_inserters is not None:
            # Workers pré-créés par l'appelant (ex: data_importer pour les tests)
            logger.info(f"[{correlation_id}] Mode pipeline: {len(worker_inserters)} workers pour {table_name}")
            own_workers = True
        elif num_workers > 1:
            logger.info(f"[{correlation_id}] Mode pipeline: {num_workers} workers pour {table_name}")
            own_workers = True
            worker_inserters = []
            for _ in range(num_workers):
                worker_hook = create_postgres_hook(bluegreen_schema=self.target_schema)
                worker_inserters.append(AMUEBatchInserter(worker_hook, target_schema=self.target_schema))
        else:
            own_workers = False
            worker_inserters = [self.inserter]

        # Build SQL par worker (chaque worker a sa propre connexion)
        insert_sqls: List[str] = []
        for w in worker_inserters:
            conn = w.get_connection()
            insert_sqls.append(
                w.build_insert_sql_for_values(table_name, columns, primary_keys, True, conn)
            )

        # Queue avec backpressure et event d'erreur
        batch_queue: queue.Queue = queue.Queue(maxsize=num_workers + 1)
        error_event = threading.Event()

        # Métriques partagées (protégées par lock)
        batch_metrics: List[Dict] = []
        metrics_lock = threading.Lock()

        def consumer(worker_idx: int) -> None:
            """Fonction consommateur exécutée dans un thread."""
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

        total_submitted = 0
        total_fetched = 0
        batch: List[tuple] = []
        batch_num = 0

        try:
            with ThreadPoolExecutor(max_workers=num_workers) as executor:
                futures: List[Future] = []
                for i in range(num_workers):
                    futures.append(executor.submit(consumer, i))

                producer_error = None
                col_key_map = None  # calculé à la première ligne : col_lower → clé réelle
                try:
                    for row in self.streamer.stream_data(table_name, import_config):
                        total_fetched += 1

                        if col_key_map is None and isinstance(row, dict):
                            _lower_to_orig = {k.lower(): k for k in row.keys()}
                            col_key_map = [_lower_to_orig.get(col) for col in data_columns]
                        if isinstance(row, dict) and col_key_map is not None:
                            data_values = [row.get(k) for k in col_key_map]
                        else:
                            data_values = [None] * len(data_columns)
                        meta_values = [PROTECTED_SOURCE, import_timestamp]
                        record = tuple(data_values + meta_values)
                        batch.append(record)

                        if len(batch) >= batch_size:
                            batch_num += 1
                            batch_copy = list(batch)
                            self._queue_put_safe(batch_queue, (batch_copy, batch_num), error_event)
                            total_submitted += len(batch)
                            logger.info(f"{table_name}: {total_submitted:,} lignes soumises")
                            batch.clear()

                    if batch:
                        batch_num += 1
                        batch_copy = list(batch)
                        self._queue_put_safe(batch_queue, (batch_copy, batch_num), error_event)
                        total_submitted += len(batch)

                    for _ in range(num_workers):
                        self._queue_put_safe(batch_queue, None, error_event)
                except Exception as e:
                    error_event.set()
                    producer_error = e

                for f in futures:
                    f.result()

                if producer_error is not None:
                    raise producer_error

            total_inserted = sum(m.get('rows_inserted', 0) for m in batch_metrics)
            total_updated = sum(m.get('rows_updated', 0) for m in batch_metrics)
            logger.info(
                f"[{correlation_id}] {table_name}: "
                f"{total_inserted:,} insérées, {total_updated:,} mises à jour "
                f"({total_fetched:,} récupérées)"
            )
            return total_inserted, total_updated, total_fetched, batch_metrics

        except (AMUEDatabaseError, AMUEBatchError, AMUEDataError):
            raise
        except Exception as e:
            from airflow.exceptions import AirflowException
            if isinstance(e, AirflowException):
                raise
            logger.error(f"[{correlation_id}] Erreur insertion {table_name}: {e}")
            # Diagnostic spécifique : ON CONFLICT sans contrainte UNIQUE correspondante
            # pgcode 42P10 = invalid_column_reference (PostgreSQL)
            pgcode = getattr(e, 'pgcode', None)
            if pgcode == '42P10' or 'on conflict' in str(e).lower():
                logger.error(f"[{correlation_id}]   → Clés primaires configurées : {primary_keys}")
                if insert_sqls:
                    logger.error(f"[{correlation_id}]   → SQL généré :\n{insert_sqls[0].strip()}")
                logger.error(
                    f"[{correlation_id}]   → Diagnostic : la table '{table_name}' n'a pas "
                    f"de contrainte UNIQUE ou PRIMARY KEY sur {primary_keys} dans PostgreSQL"
                )
                logger.error(
                    f"[{correlation_id}]   → Action requise : relancer amue_table_setup "
                    f"pour recréer la table avec la bonne contrainte sur {primary_keys}"
                )
            raise AMUEDatabaseError(
                f"Erreur d'insertion pour {table_name}: {e}",
                table_name=table_name,
                correlation_id=correlation_id
            ) from e
        finally:
            if own_workers:
                for w in worker_inserters:
                    w.close_connection()
