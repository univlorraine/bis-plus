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
import logging
from string import Template
from typing import Dict, List

from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from amue.operators.data_streamer import AMUEDataStreamer
from amue.operators.batch_inserter import AMUEBatchInserter
from amue.utils.airflow_helpers import AirflowVariableManager as VarMgr

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

    def __init__(self, api_hook, postgres_hook: PostgresHook = None):
        """
        Initialise l'importeur de donnees AMUE.

        Args:
            api_hook: Instance de AMUEAPIHook pour les appels API
            postgres_hook: Hook PostgreSQL optionnel (cree si non fourni)

        Raises:
            AirflowException: Si les variables obligatoires sont manquantes
        """
        self.api_hook = api_hook

        # Connexion PostgreSQL avec schema splus par defaut
        self.postgres_hook = postgres_hook or PostgresHook(
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

        # Initialisation des sous-services
        self.streamer = AMUEDataStreamer(api_hook, self.endpoint)
        self.inserter = AMUEBatchInserter(self.postgres_hook)

    def import_table(
        self,
        table_name: str,
        columns: List[str],
        primary_keys: List[str],
        import_config: Dict
    ) -> Dict:
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
                - finger_print: Empreinte de structure de la table

        Returns:
            Dictionnaire avec le resultat de l'import :
            {
                "table_name": "CSKS",
                "rows_inserted": 1500,
                "rows_fetched": 1500,
                "import_type": "full",
                "finger_print": "abc123...",
                "status": "success"
            }

        Raises:
            AirflowException: Si l'import echoue (erreur API, DB, doublons...)
        """
        logger.info(f"Table: {table_name}, type: {import_config.get('import_type', 'full')}")

        try:
            # Determine la strategie d'insertion
            import_type = import_config.get('import_type', 'full')
            use_upsert = import_type == 'differential' and bool(primary_keys)

            # Lance le streaming et l'insertion par batch
            rows_inserted, rows_fetched = self._stream_and_insert(
                table_name,
                columns,
                primary_keys,
                import_config,
                use_upsert
            )

            return {
                'table_name': table_name,
                'rows_inserted': rows_inserted,
                'rows_fetched': rows_fetched,
                'import_type': import_type,
                'finger_print': import_config.get('finger_print', ''),
                'status': 'success'
            }

        except Exception as e:
            logger.error(f"Erreur import {table_name}: {e}")
            raise
        finally:
            self.inserter.close_connection()

    def _stream_and_insert(
        self,
        table_name: str,
        columns: List[str],
        primary_keys: List[str],
        import_config: Dict,
        use_upsert: bool
    ) -> tuple:
        """
        Orchestre le streaming des donnees et leur insertion par batch.

        Cette methode implemente deux strategies transactionnelles :

        IMPORT FULL (transaction atomique) :
            BEGIN -> TRUNCATE -> INSERT batch 1 -> INSERT batch 2 -> ... -> COMMIT
            En cas d'erreur : ROLLBACK -> donnees originales intactes

        IMPORT DIFFERENTIAL (commits par batch) :
            UPSERT batch 1 -> COMMIT -> UPSERT batch 2 -> COMMIT -> ...
            Plus resilient : les batchs deja committes sont conserves

        Args:
            table_name: Nom de la table cible
            columns: Liste des colonnes
            primary_keys: Cles primaires pour UPSERT
            import_config: Configuration d'import
            use_upsert: True pour UPSERT, False pour INSERT simple

        Returns:
            Tuple (rows_inserted, rows_fetched)
        """
        conn = self.inserter.get_connection()
        cursor = conn.cursor()

        import_type = import_config.get('import_type', 'full')
        is_full_import = import_type == 'full'

        # FULL IMPORT : TRUNCATE dans la meme transaction
        if is_full_import:
            self.inserter.truncate_table(cursor, table_name)

        # Prepare la requete SQL
        insert_sql = self.inserter.build_insert_sql(
            table_name, columns, primary_keys, use_upsert, conn
        )

        total_inserted = 0
        total_fetched = 0
        batch = []

        try:
            # STREAMING : recupere les donnees ligne par ligne
            for row in self.streamer.stream_data(table_name, import_config):
                total_fetched += 1

                # Normalise les cles en minuscules
                row_lower = {k.lower(): v for k, v in row.items()} if isinstance(row, dict) else {}

                # Construit le tuple dans l'ordre des colonnes attendues
                record = tuple(row_lower.get(col, None) for col in columns)
                batch.append(record)

                # BATCH PLEIN : insertion immediate
                if len(batch) >= self.batch_size:
                    self.inserter.execute_batch(
                        cursor, conn, insert_sql, batch,
                        table_name, columns, primary_keys,
                        commit=not is_full_import
                    )
                    total_inserted += len(batch)
                    logger.info(f"{table_name}: {total_inserted:,} lignes inserees")
                    batch.clear()

            # RESTE DU BATCH : dernieres lignes
            if batch:
                self.inserter.execute_batch(
                    cursor, conn, insert_sql, batch,
                    table_name, columns, primary_keys,
                    commit=not is_full_import
                )
                total_inserted += len(batch)

            # FULL IMPORT : commit final de toute la transaction
            if is_full_import:
                conn.commit()
                logger.info(f"[FULL IMPORT] Transaction commitee pour {table_name}")

            logger.info(f"{table_name}: Total {total_inserted:,}/{total_fetched:,} lignes")
            return total_inserted, total_fetched

        except AirflowException:
            conn.rollback()
            if is_full_import:
                logger.warning(f"[FULL IMPORT] Rollback complet pour {table_name} - donnees originales preservees")
            raise
        except Exception as e:
            conn.rollback()
            if is_full_import:
                logger.warning(f"[FULL IMPORT] Rollback complet pour {table_name} - donnees originales preservees")
            logger.error(f"Erreur insertion {table_name} apres {total_inserted} lignes: {e}")
            raise AirflowException(f"Import error: {e}")
        finally:
            cursor.close()
