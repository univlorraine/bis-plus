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

        # Initialisation des sous-services avec le schéma cible
        self.streamer = AMUEDataStreamer(api_hook, self.endpoint)
        self.inserter = AMUEBatchInserter(self.postgres_hook, target_schema=target_schema)

        # Source par défaut pour les meta colonnes
        self.default_source = VarMgr.get('amue_default_source', default='sifac_plus')

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

    def _get_text_columns(self, table_name: str, columns: List[str]) -> set:
        """
        Récupère les colonnes de type texte depuis information_schema.

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
            return text_cols
        except Exception as e:
            logger.warning(f"[IMPORT] Impossible de récupérer les types de colonnes pour {table_name}: {e}")
            # En cas d'erreur, on considère toutes les colonnes comme texte
            # pour ne pas casser l'import (pas de conversion '' -> NULL)
            return set(columns)

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
                rows_inserted, rows_fetched = self._stream_and_insert(
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

                return {
                    'table_name': table_name,
                    'rows_inserted': rows_inserted,
                    'rows_fetched': rows_fetched,
                    'import_type': import_type,
                    'finger_print': import_config.get('finger_print', ''),
                    'status': 'success',
                    'correlation_id': correlation_id
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
    ) -> Tuple[int, int]:
        """
        Orchestre le streaming des donnees et leur insertion par batch.

        UPSERT UNIQUEMENT - Plus de TRUNCATE/INSERT :
            UPSERT batch 1 -> COMMIT -> UPSERT batch 2 -> COMMIT -> ...
            Resilient : les batchs deja committes sont conserves
            Pas de perte de données : les données existantes sont préservées

        Args:
            table_name: Nom de la table cible
            columns: Liste des colonnes (incluant _source et _imported_at)
            primary_keys: Cles primaires pour UPSERT
            import_config: Configuration d'import
            use_upsert: Toujours True (conservé pour compatibilité)
            correlation_id: ID de corrélation pour le tracing

        Returns:
            Tuple (rows_inserted, rows_fetched)

        Raises:
            AMUEDatabaseError: En cas d'erreur de connexion DB
            AMUEBatchError: En cas d'erreur d'insertion batch
            AMUEDataError: En cas de données invalides
        """
        conn = self.inserter.get_connection()
        cursor = conn.cursor()

        # Plus de TRUNCATE - UPSERT uniquement
        # Les colonnes de données (sans _source et _imported_at)
        data_columns = [c for c in columns if c not in ('_source', '_imported_at')]

        # Récupère les types des colonnes pour savoir où convertir '' en NULL
        text_columns = self._get_text_columns(table_name, data_columns)

        # Prepare la requete SQL avec meta colonnes
        insert_sql = self.inserter.build_insert_sql(
            table_name, columns, primary_keys, use_upsert, conn
        )

        total_inserted = 0
        total_fetched = 0
        batch = []
        import_timestamp = datetime.now()

        try:
            # STREAMING : recupere les donnees ligne par ligne
            for row in self.streamer.stream_data(table_name, import_config):
                total_fetched += 1

                # Normalise les cles en minuscules
                row_lower = {k.lower(): v for k, v in row.items()} if isinstance(row, dict) else {}

                # Construit le tuple dans l'ordre des colonnes attendues (données + meta)
                # Convertit '' en NULL uniquement pour les colonnes non-texte
                data_values = [
                    None if row_lower.get(col, None) == '' and col not in text_columns
                    else row_lower.get(col, None)
                    for col in data_columns
                ]
                # Ajoute les valeurs des meta colonnes
                meta_values = [self.default_source, import_timestamp]
                record = tuple(data_values + meta_values)
                batch.append(record)

                # BATCH PLEIN : insertion immediate avec commit
                if len(batch) >= self.batch_size:
                    self.inserter.execute_batch(
                        cursor, conn, insert_sql, batch,
                        table_name, columns, primary_keys,
                        commit=True  # Toujours commit par batch
                    )
                    total_inserted += len(batch)
                    logger.info(f"{table_name}: {total_inserted:,} lignes inserees (UPSERT)")
                    batch.clear()

            # RESTE DU BATCH : dernieres lignes
            if batch:
                self.inserter.execute_batch(
                    cursor, conn, insert_sql, batch,
                    table_name, columns, primary_keys,
                    commit=True
                )
                total_inserted += len(batch)

            logger.info(f"[{correlation_id}] {table_name}: Total {total_inserted:,}/{total_fetched:,} lignes (UPSERT)")
            return total_inserted, total_fetched

        except (AMUEDatabaseError, AMUEBatchError, AMUEDataError):
            # Les exceptions AMUE spécifiques sont propagées
            conn.rollback()
            logger.warning(f"[{correlation_id}] Rollback du batch en cours pour {table_name}")
            raise
        except AirflowException:
            conn.rollback()
            logger.warning(f"[{correlation_id}] Rollback du batch en cours pour {table_name}")
            raise
        except Exception as e:
            conn.rollback()
            logger.warning(f"[{correlation_id}] Rollback du batch en cours pour {table_name}")
            logger.error(f"[{correlation_id}] Erreur insertion {table_name} apres {total_inserted} lignes: {e}")
            raise AMUEDatabaseError(
                f"Erreur d'insertion pour {table_name} après {total_inserted} lignes: {e}",
                table_name=table_name,
                correlation_id=correlation_id
            ) from e
        finally:
            cursor.close()
