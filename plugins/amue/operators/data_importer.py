"""
Gestionnaire d'import des données depuis l'API AMUE
Avec streaming et insertion par batch pour optimiser la mémoire
"""
import logging
import re
from datetime import datetime
from string import Template
from typing import Dict, List, Generator, Optional, Tuple

import requests
from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from amue.services.retry_service import get_retry_service, ErrorCategory
from amue.utils.airflow_helpers import AirflowVariableManager as VarMgr
from psycopg2 import sql
from psycopg2.errors import UniqueViolation

logger = logging.getLogger(__name__)


class AMUEDataImporter:
    """Gère l'import des données depuis l'API vers PostgreSQL avec streaming"""

    # Taille de batch par défaut pour l'insertion
    DEFAULT_BATCH_SIZE = 5000

    def __init__(self, api_hook, postgres_hook: PostgresHook = None):
        self.api_hook = api_hook
        self.postgres_hook = postgres_hook or PostgresHook(
            postgres_conn_id='postgres_data',
            options='-c search_path=splus'
        )
        self._conn = None  # Cache de connexion

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

    def _get_connection(self):
        """Retourne une connexion réutilisable"""
        if self._conn is None or self._conn.closed:
            self._conn = self.postgres_hook.get_conn()
        return self._conn

    def _close_connection(self):
        """Ferme proprement la connexion"""
        if self._conn and not self._conn.closed:
            self._conn.close()
            self._conn = None

    def _truncate_table_no_commit(self, cursor, table_name: str) -> None:
        """
        Vide la table avant un import complet (FULL) - SANS COMMIT

        Le commit sera fait à la fin de l'import complet pour garantir
        l'atomicité de l'opération (TRUNCATE + INSERT dans une seule transaction).

        Utilise TRUNCATE pour une suppression rapide et efficace.
        CASCADE est utilisé pour gérer les éventuelles contraintes FK.

        Args:
            cursor: Curseur de la connexion
            table_name: Nom de la table à vider
        """
        # Utilise sql.Identifier pour éviter les injections SQL
        truncate_sql = sql.SQL("TRUNCATE TABLE {table} CASCADE").format(
            table=sql.Identifier(table_name)
        )
        cursor.execute(truncate_sql)
        logger.info(f"[FULL IMPORT] TRUNCATE préparé pour {table_name} (en attente du commit final)")

    def import_table(self, table_name: str, columns: List[str], primary_keys: List[str],
                     import_config: Dict) -> Dict:
        """Importe les données d'une table avec streaming"""
        logger.info(f"Table: {table_name}, type: {import_config.get('import_type', 'full')}")

        try:
            # Détermine si on utilise UPSERT (vérifie avant le stream)
            import_type = import_config.get('import_type', 'full')
            use_upsert = import_type == 'differential' and bool(primary_keys)

            # Stream les données et insère par batch
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
            self._close_connection()

    def _stream_and_insert(self, table_name: str, columns: List[str],
                           primary_keys: List[str], import_config: Dict,
                           use_upsert: bool) -> tuple:
        """
        Stream les données depuis l'API et insère par batch

        Pour un import FULL: utilise une transaction unique (TRUNCATE + INSERT)
        afin de garantir un rollback complet en cas d'erreur.

        Pour un import DIFFERENTIAL: commit par batch (UPSERT idempotent).

        Returns:
            Tuple (rows_inserted, rows_fetched)
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        import_type = import_config.get('import_type', 'full')
        is_full_import = import_type == 'full'

        # Pour un import FULL, vide la table (dans la même transaction)
        if is_full_import:
            self._truncate_table_no_commit(cursor, table_name)

        # Construit la requête SQL
        insert_sql = self._build_insert_sql(table_name, columns, primary_keys, use_upsert)

        total_inserted = 0
        total_fetched = 0
        batch = []

        try:
            # Stream les données depuis l'API
            for row in self._fetch_data_stream(table_name, import_config):
                total_fetched += 1

                # Prépare le record
                row_lower = {k.lower(): v for k, v in row.items()} if isinstance(row, dict) else {}
                record = tuple(row_lower.get(col, None) for col in columns)
                batch.append(record)

                # Insert quand batch plein
                if len(batch) >= self.batch_size:
                    self._execute_batch(
                        cursor, conn, insert_sql, batch,
                        table_name, columns, primary_keys,
                        commit=not is_full_import  # Pas de commit intermédiaire pour FULL
                    )
                    total_inserted += len(batch)
                    logger.info(f"{table_name}: {total_inserted:,} lignes insérées")
                    batch.clear()  # Libère mémoire

            # Insert reste du batch
            if batch:
                self._execute_batch(
                    cursor, conn, insert_sql, batch,
                    table_name, columns, primary_keys,
                    commit=not is_full_import
                )
                total_inserted += len(batch)

            # Pour FULL import: commit final de toute la transaction
            if is_full_import:
                conn.commit()
                logger.info(f"[FULL IMPORT] Transaction commitée pour {table_name}")

            logger.info(f"{table_name}: Total {total_inserted:,}/{total_fetched:,} lignes")
            return total_inserted, total_fetched

        except AirflowException:
            conn.rollback()
            if is_full_import:
                logger.warning(f"[FULL IMPORT] Rollback complet pour {table_name} - données originales préservées")
            raise
        except Exception as e:
            conn.rollback()
            if is_full_import:
                logger.warning(f"[FULL IMPORT] Rollback complet pour {table_name} - données originales préservées")
            logger.error(f"Erreur insertion {table_name} après {total_inserted} lignes: {e}")
            raise AirflowException(f"Import error: {e}")
        finally:
            cursor.close()

    def _execute_batch(
            self,
            cursor,
            conn,
            insert_sql: str,
            batch: List[tuple],
            table_name: str,
            columns: List[str],
            primary_keys: List[str],
            commit: bool = True
    ) -> None:
        """
        Exécute un batch d'insertions avec détection des conflits de clé primaire

        En cas de conflit, affiche les données des deux lignes (existante et nouvelle)
        pour faciliter le diagnostic.

        Args:
            cursor: Curseur de la connexion
            conn: Connexion à la base de données
            insert_sql: Requête SQL d'insertion
            batch: Liste des tuples à insérer
            table_name: Nom de la table
            columns: Liste des colonnes
            primary_keys: Liste des clés primaires
            commit: Si True, commit après l'insertion (False pour transaction globale)
        """
        # Détection proactive des doublons dans le batch AVANT insertion
        if primary_keys:
            duplicates_found = self._detect_duplicates_in_batch(batch, columns, primary_keys)
            if duplicates_found:
                self._log_all_batch_duplicates(table_name, columns, primary_keys, duplicates_found)
                raise AirflowException(
                    f"Doublons détectés dans les données API pour {table_name}. "
                    f"{len(duplicates_found)} groupe(s) de doublons trouvé(s). "
                    f"Voir les logs pour les détails."
                )

        try:
            cursor.executemany(insert_sql, batch)
            if commit:
                conn.commit()

        except UniqueViolation as e:
            # Pas de rollback ici - laisse le caller gérer la transaction
            if commit:
                conn.rollback()

            # Log l'erreur originale
            logger.error(f"[CONFLIT PK] Erreur de clé primaire dupliquée sur {table_name}")
            logger.error(f"[CONFLIT PK] Message: {e.pgerror}")

            # Extrait les valeurs de la clé primaire depuis le message d'erreur
            pk_values = self._extract_pk_values_from_error(str(e.pgerror), primary_keys)

            if pk_values and primary_keys:
                # Cherche d'abord les doublons dans le batch (cas API avec doublons)
                duplicates_in_batch = self._find_all_duplicates_in_batch(
                    batch, columns, primary_keys, pk_values
                )

                if len(duplicates_in_batch) > 1:
                    # Les doublons viennent des données de l'API
                    self._log_api_duplicates(
                        table_name, columns, primary_keys, duplicates_in_batch, pk_values
                    )
                else:
                    # Le conflit est avec une ligne existante en base
                    existing_row = self._fetch_existing_row(
                        cursor, conn, table_name, columns, primary_keys, pk_values
                    )
                    conflicting_row = duplicates_in_batch[0] if duplicates_in_batch else None

                    self._log_conflict_details(
                        table_name, columns, primary_keys,
                        existing_row, conflicting_row, pk_values
                    )

            raise AirflowException(
                f"Conflit de clé primaire sur {table_name}. "
                f"Voir les logs pour les détails des lignes en conflit."
            )

    def _extract_pk_values_from_error(
            self,
            error_message: str,
            primary_keys: List[str]
    ) -> Optional[Dict[str, str]]:
        """
        Extrait les valeurs de clé primaire depuis le message d'erreur PostgreSQL

        Le message ressemble à:
        DETAIL: La clé « (col1, col2)=(val1, val2) » existe déjà.
        """
        try:
            # Pattern pour extraire les colonnes et valeurs
            # Format: (col1, col2, ...)=(val1, val2, ...)
            pattern = r'\(([^)]+)\)=\(([^)]+)\)'
            match = re.search(pattern, error_message)

            if not match:
                logger.warning("[CONFLIT PK] Impossible d'extraire les valeurs PK du message d'erreur")
                return None

            cols_str = match.group(1)
            vals_str = match.group(2)

            # Parse les colonnes
            cols = [c.strip() for c in cols_str.split(',')]

            # Parse les valeurs (attention aux virgules dans les valeurs)
            vals = self._parse_pk_values(vals_str)

            if len(cols) != len(vals):
                logger.warning(f"[CONFLIT PK] Mismatch colonnes/valeurs: {len(cols)} vs {len(vals)}")
                return None

            pk_values = dict(zip(cols, vals))
            logger.info(f"[CONFLIT PK] Clé primaire en conflit: {pk_values}")

            return pk_values

        except Exception as e:
            logger.warning(f"[CONFLIT PK] Erreur extraction PK: {e}")
            return None

    def _parse_pk_values(self, vals_str: str) -> List[str]:
        """
        Parse les valeurs de clé primaire depuis la chaîne

        Gère les cas où les valeurs contiennent des virgules
        """
        values = []
        current = ""
        in_quotes = False

        for char in vals_str:
            if char == "'" or char == '"':
                in_quotes = not in_quotes
            elif char == ',' and not in_quotes:
                values.append(current.strip())
                current = ""
                continue
            current += char

        if current:
            values.append(current.strip())

        return values

    def _fetch_existing_row(
            self,
            cursor,
            conn,
            table_name: str,
            columns: List[str],
            primary_keys: List[str],
            pk_values: Dict[str, str]
    ) -> Optional[Dict[str, any]]:
        """
        Récupère la ligne existante en base avec les valeurs de clé primaire
        """
        try:
            # Construit la requête SELECT
            where_clauses = []
            params = []

            for pk in primary_keys:
                pk_lower = pk.lower()
                if pk_lower in pk_values:
                    where_clauses.append(f"{pk_lower} = %s")
                    params.append(pk_values[pk_lower])
                elif pk in pk_values:
                    where_clauses.append(f"{pk.lower()} = %s")
                    params.append(pk_values[pk])

            if not where_clauses:
                return None

            select_sql = f"""
                SELECT {', '.join(columns)}
                FROM {table_name}
                WHERE {' AND '.join(where_clauses)}
            """

            cursor.execute(select_sql, params)
            row = cursor.fetchone()

            if row:
                return dict(zip(columns, row))

            return None

        except Exception as e:
            logger.warning(f"[CONFLIT PK] Erreur récupération ligne existante: {e}")
            return None

    def _find_conflicting_row_in_batch(
            self,
            batch: List[tuple],
            columns: List[str],
            primary_keys: List[str],
            pk_values: Dict[str, str]
    ) -> Optional[Dict[str, any]]:
        """
        Trouve la première ligne en conflit dans le batch actuel
        """
        duplicates = self._find_all_duplicates_in_batch(batch, columns, primary_keys, pk_values)
        return duplicates[0] if duplicates else None

    def _find_all_duplicates_in_batch(
            self,
            batch: List[tuple],
            columns: List[str],
            primary_keys: List[str],
            pk_values: Dict[str, str]
    ) -> List[Dict[str, any]]:
        """
        Trouve TOUTES les lignes avec la même clé primaire dans le batch

        Utile pour détecter les doublons dans les données de l'API
        """
        duplicates = []

        try:
            # Crée un index des colonnes PK
            pk_indices = []
            for pk in primary_keys:
                pk_lower = pk.lower()
                if pk_lower in columns:
                    pk_indices.append((pk_lower, columns.index(pk_lower)))

            # Cherche toutes les correspondances dans le batch
            for idx, record in enumerate(batch):
                match = True
                for pk_name, pk_idx in pk_indices:
                    record_val = str(record[pk_idx]).strip() if record[pk_idx] is not None else ""
                    expected_val = pk_values.get(pk_name, pk_values.get(pk_name.upper(), "")).strip()

                    if record_val != expected_val:
                        match = False
                        break

                if match:
                    row_dict = dict(zip(columns, record))
                    row_dict['_batch_index'] = idx  # Ajoute l'index pour référence
                    duplicates.append(row_dict)

            return duplicates

        except Exception as e:
            logger.warning(f"[CONFLIT PK] Erreur recherche doublons dans batch: {e}")
            return []

    def _detect_duplicates_in_batch(
            self,
            batch: List[tuple],
            columns: List[str],
            primary_keys: List[str]
    ) -> Dict[str, List[Dict[str, any]]]:
        """
        Détecte TOUS les doublons de clé primaire dans le batch AVANT insertion

        Returns:
            Dict avec clé PK (stringifiée) -> liste des lignes en doublon
            Seuls les groupes avec plus d'une ligne sont retournés
        """
        try:
            # Crée un index des colonnes PK
            pk_indices = []
            for pk in primary_keys:
                pk_lower = pk.lower()
                if pk_lower in columns:
                    pk_indices.append((pk_lower, columns.index(pk_lower)))

            if not pk_indices:
                return {}

            # Groupe les lignes par clé primaire
            pk_groups: Dict[str, List[Dict[str, any]]] = {}

            for idx, record in enumerate(batch):
                # Construit la clé PK
                pk_key_parts = []
                pk_values = {}
                for pk_name, pk_idx in pk_indices:
                    val = str(record[pk_idx]).strip() if record[pk_idx] is not None else ""
                    pk_key_parts.append(f"{pk_name}={val}")
                    pk_values[pk_name] = val

                pk_key = "|".join(pk_key_parts)

                # Ajoute au groupe
                if pk_key not in pk_groups:
                    pk_groups[pk_key] = []

                row_dict = dict(zip(columns, record))
                row_dict['_batch_index'] = idx
                row_dict['_pk_values'] = pk_values
                pk_groups[pk_key].append(row_dict)

            # Retourne uniquement les groupes avec doublons (plus d'une ligne)
            return {k: v for k, v in pk_groups.items() if len(v) > 1}

        except Exception as e:
            logger.warning(f"[CONFLIT PK] Erreur détection doublons: {e}")
            return {}

    def _log_all_batch_duplicates(
            self,
            table_name: str,
            columns: List[str],
            primary_keys: List[str],
            duplicates_groups: Dict[str, List[Dict[str, any]]]
    ) -> None:
        """
        Affiche tous les groupes de doublons trouvés dans le batch
        """
        separator = "=" * 100
        total_duplicates = sum(len(group) for group in duplicates_groups.values())

        logger.error(separator)
        logger.error(f"DOUBLONS DÉTECTÉS DANS LES DONNÉES API - Table: {table_name}")
        logger.error(f"ATTENTION: {len(duplicates_groups)} groupe(s) de doublons, {total_duplicates} lignes au total!")
        logger.error(separator)

        for group_idx, (pk_key, duplicates) in enumerate(duplicates_groups.items(), 1):
            logger.error(f"{'='*50}")
            logger.error(f"GROUPE DE DOUBLONS #{group_idx} ({len(duplicates)} lignes)")
            logger.error(f"{'='*50}")

            # Affiche la clé primaire
            if duplicates and '_pk_values' in duplicates[0]:
                pk_values = duplicates[0]['_pk_values']
                logger.error(f"Clé primaire dupliquée:")
                for pk in primary_keys:
                    pk_lower = pk.lower()
                    val = pk_values.get(pk_lower, "N/A")
                    logger.error(f"  {pk}: '{val}'")

            # Compare les doublons
            logger.error(f"Comparaison des {len(duplicates)} lignes:")
            logger.error("-" * 100)

            # En-tête
            header = f"{'COLONNE':<30}"
            for i in range(len(duplicates)):
                header += f" {'LIGNE #' + str(i+1):<25}"
            logger.error(header)
            logger.error("-" * 100)

            # Colonnes avec différences
            cols_with_diff = []
            for col in columns:
                values = [str(d.get(col, "")) for d in duplicates]
                row_str = f"  {col:<28}"

                for val in values:
                    val_display = val[:22] + "..." if len(val) > 25 else val
                    row_str += f" {val_display:<25}"

                if len(set(values)) > 1:
                    row_str += " **DIFF**"
                    cols_with_diff.append(col)

                logger.error(row_str)

            if cols_with_diff:
                logger.error(f"  -> Colonnes différentes: {', '.join(cols_with_diff)}")
            else:
                logger.error(f"  -> DOUBLONS IDENTIQUES (lignes 100% identiques)")

        logger.error(f"{separator}")
        logger.error("RÉSUMÉ:")
        logger.error(f"  - {len(duplicates_groups)} groupe(s) de doublons")
        logger.error(f"  - {total_duplicates} lignes en doublon au total")
        logger.error(f"ACTION RECOMMANDÉE:")
        logger.error("  1. Vérifier les données source dans l'API AMUE")
        logger.error("  2. Contacter l'administrateur AMUE pour signaler les doublons")
        logger.error("  3. OU utiliser l'option UPSERT (import différentiel) pour gérer les doublons")
        logger.error(separator)

    def _log_api_duplicates(
            self,
            table_name: str,
            columns: List[str],
            primary_keys: List[str],
            duplicates: List[Dict[str, any]],
            pk_values: Dict[str, str]
    ) -> None:
        """
        Affiche les doublons trouvés dans les données de l'API
        """
        separator = "=" * 100
        logger.error(separator)
        logger.error(f"DOUBLONS DANS LES DONNÉES API - Table: {table_name}")
        logger.error(f"ATTENTION: L'API a renvoyé {len(duplicates)} lignes avec la même clé primaire!")
        logger.error(separator)

        logger.error(f"Clé primaire dupliquée:")
        for pk in primary_keys:
            pk_lower = pk.lower()
            val = pk_values.get(pk_lower, pk_values.get(pk, "N/A"))
            logger.error(f"  {pk}: '{val}'")

        logger.error(f"{len(duplicates)} LIGNES EN DOUBLON TROUVÉES:")
        logger.error("-" * 100)

        # Affiche chaque ligne en doublon
        for i, row in enumerate(duplicates, 1):
            batch_idx = row.pop('_batch_index', 'N/A')
            logger.error(f"--- Doublon #{i} (index batch: {batch_idx}) ---")

            for col in columns:
                val = row.get(col, "N/A")
                val_str = str(val)[:60] + "..." if len(str(val)) > 60 else str(val)
                logger.error(f"  {col:<30}: {val_str}")

        # Compare les doublons pour voir les différences
        if len(duplicates) >= 2:
            logger.error(f"{separator}")
            logger.error("COMPARAISON DES DOUBLONS:")
            logger.error("-" * 100)

            # En-tête
            header = f"{'COLONNE':<30}"
            for i in range(len(duplicates)):
                header += f" {'DOUBLON #' + str(i+1):<25}"
            logger.error(header)
            logger.error("-" * 100)

            # Colonnes avec différences
            cols_with_diff = []
            for col in columns:
                values = [str(d.get(col, "")) for d in duplicates]
                row_str = f"  {col:<28}"

                for val in values:
                    val_display = val[:22] + "..." if len(val) > 25 else val
                    row_str += f" {val_display:<25}"

                # Vérifie si les valeurs sont différentes
                if len(set(values)) > 1:
                    row_str += " **DIFF**"
                    cols_with_diff.append(col)

                logger.error(row_str)

            logger.error("-" * 100)

            if cols_with_diff:
                logger.error(f"\Colonnes avec différences ({len(cols_with_diff)}): {', '.join(cols_with_diff)}")
            else:
                logger.error("Tous les doublons sont IDENTIQUES (doublons exacts dans l'API)")

        logger.error(separator)
        logger.error("ACTION RECOMMANDÉE:")
        logger.error("  1. Vérifier les données source dans l'API AMUE")
        logger.error("  2. Contacter l'administrateur AMUE si les doublons ne devraient pas exister")
        logger.error("  3. Utiliser l'option UPSERT (import différentiel) pour ignorer les doublons")
        logger.error(separator)

    def _log_conflict_details(
            self,
            table_name: str,
            columns: List[str],
            primary_keys: List[str],
            existing_row: Optional[Dict],
            new_row: Optional[Dict],
            pk_values: Dict[str, str]
    ) -> None:
        """
        Affiche les détails du conflit de manière lisible
        """
        separator = "=" * 80
        logger.error(separator)
        logger.error(f"CONFLIT DE CLÉ PRIMAIRE - Table: {table_name}")
        logger.error(separator)

        logger.error(f"Clé primaire en conflit:")
        for pk in primary_keys:
            pk_lower = pk.lower()
            val = pk_values.get(pk_lower, pk_values.get(pk, "N/A"))
            logger.error(f"  {pk}: '{val}'")

        logger.error(f"{'COLONNE':<30} {'EXISTANTE (BD)':<40} {'NOUVELLE (API)':<40}")
        logger.error("-" * 110)

        for col in columns:
            existing_val = existing_row.get(col, "N/A") if existing_row else "N/A"
            new_val = new_row.get(col, "N/A") if new_row else "N/A"

            # Tronque les valeurs longues pour l'affichage
            existing_str = str(existing_val)[:37] + "..." if len(str(existing_val)) > 40 else str(existing_val)
            new_str = str(new_val)[:37] + "..." if len(str(new_val)) > 40 else str(new_val)

            # Marque les différences
            marker = " **" if str(existing_val) != str(new_val) else ""

            logger.error(f"  {col:<28} {existing_str:<40} {new_str:<40}{marker}")

        logger.error(separator)

        # Résumé des différences
        if existing_row and new_row:
            differences = []
            for col in columns:
                if str(existing_row.get(col, "")) != str(new_row.get(col, "")):
                    differences.append(col)

            if differences:
                logger.error(f"Colonnes différentes ({len(differences)}): {', '.join(differences)}")
            else:
                logger.error("Les deux lignes sont IDENTIQUES (doublon exact dans les données API)")

        logger.error(separator)

    def _fetch_data_stream(self, table_name: str, import_config: Dict) -> Generator[Dict, None, None]:
        """
        Récupère les données en streaming (générateur)

        Yields:
            Ligne de données une par une
        """
        base_params = self._build_query_params(table_name, import_config)
        skip = 0
        page = 1

        while True:
            params = base_params.copy()
            params['skip'] = skip

            rows, has_more = self._fetch_page(params, page)

            if not rows:
                break

            for row in rows:
                yield row  # Yield au lieu d'accumuler

            if not has_more:
                break

            skip += len(rows)
            page += 1

    def _build_query_params(self, table_name: str, import_config: Dict) -> Dict:
        """Construit les paramètres de requête"""
        params = {
            'nom': table_name.upper(),
            'f': 'json'
        }

        # Import différentiel
        import_type = import_config.get('import_type', 'full')
        delta_column = import_config.get('delta', '')
        last_import = import_config.get('last_import', '')

        if import_type == 'differential' and delta_column and last_import:
            last_import_str = self._format_date_for_query(last_import)
            params['q'] = f"{delta_column}='{last_import_str}'"
            logger.info(f"Delta: {params['q']}")

        return params

    def _format_date_for_query(self, date_str: str) -> str:
        """Formate une date pour la requête"""
        try:
            dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            return dt.strftime('%Y%m%d')
        except (ValueError, AttributeError) as e:
            logger.warning(f"Format de date invalide '{date_str}': {e}")
            return date_str.replace('-', '')[:8]

    def _fetch_page(self, params: Dict, page: int) -> tuple:
        """
        Récupère une page de données avec retry intelligent

        Utilise le RetryService pour appliquer des stratégies différenciées
        selon le type d'erreur (4xx, 429, 5xx, timeout, etc.)
        """
        retry_service = get_retry_service()

        def fetch_operation():
            logger.info(f"Page {page} (skip={params['skip']})")

            response = self.api_hook.call_api(self.endpoint, params)

            if not isinstance(response, dict) or 'data' not in response:
                raise ValueError("Format réponse invalide")

            return response

        def on_retry(attempt: int, error: Exception, delay: float):
            category = retry_service.categorize_error(error)
            logger.warning(
                f"[RETRY] Page {page} - Tentative {attempt + 1} échouée - "
                f"Type: {category.value} - Retry dans {delay:.1f}s"
            )

        result = retry_service.execute_with_retry(fetch_operation, on_retry)

        if not result.success:
            # Log détaillé selon la catégorie d'erreur
            category = result.error_category
            retry_info = retry_service.get_retry_info(result.error)

            if category == ErrorCategory.CLIENT_ERROR:
                error_msg = (
                    f"Erreur client (4xx) - Pas de retry automatique. "
                    f"Vérifiez les paramètres: {result.error}"
                )
            elif category == ErrorCategory.RATE_LIMITED:
                error_msg = (
                    f"Rate limit (429) atteint après {result.attempts} tentatives. "
                    f"Temps total d'attente: {result.total_delay:.1f}s"
                )
            elif category == ErrorCategory.SERVER_ERROR:
                error_msg = (
                    f"Erreur serveur (5xx) persistante après {result.attempts} tentatives. "
                    f"L'API AMUE est peut-être indisponible."
                )
            elif category == ErrorCategory.TIMEOUT:
                error_msg = (
                    f"Timeout réseau après {result.attempts} tentatives. "
                    f"Vérifiez la connectivité."
                )
            else:
                error_msg = (
                    f"Impossible de récupérer les données après {result.attempts} tentatives: "
                    f"{result.error}"
                )

            logger.error(f"[FETCH] {error_msg}")
            logger.info(f"[FETCH] Recommandation: {retry_info['recommendation']}")
            raise AirflowException(error_msg)

        # Succès - traite la réponse
        response = result.result
        data_obj = response['data']
        rows = data_obj.get('row', [])

        if not isinstance(rows, list):
            rows = [rows] if rows else []

        if rows:
            logger.info(f"{len(rows)} lignes récupérées")

        if result.attempts > 1:
            logger.info(
                f"[FETCH] Succès après {result.attempts} tentatives "
                f"(délai total: {result.total_delay:.1f}s)"
            )

        # Vérifie s'il y a plus de données
        count = data_obj.get('count', 0)
        top = data_obj.get('top', 99)
        has_more = len(rows) >= top and (params['skip'] + len(rows)) < count

        return rows, has_more

    def _build_insert_sql(self, table_name: str, columns: List[str],
                          primary_keys: List[str], use_upsert: bool) -> str:
        """Construit la requête SQL d'insertion avec identifiants sécurisés"""
        # Identifiants sécurisés (protection SQL injection)
        table_id = sql.Identifier(table_name)
        column_ids = [sql.Identifier(col) for col in columns]

        placeholders = sql.SQL(', ').join([sql.Placeholder()] * len(columns))
        column_list = sql.SQL(', ').join(column_ids)

        if use_upsert and primary_keys:
            pk_ids = [sql.Identifier(pk) for pk in primary_keys]
            update_cols = [sql.Identifier(col) for col in columns if col not in primary_keys]

            query = sql.SQL("""
                            INSERT INTO {table} ({columns})
                            VALUES ({placeholders}) ON CONFLICT ({pks})
                            DO UPDATE SET {updates}
                            """).format(
                table=table_id,
                columns=column_list,
                placeholders=placeholders,
                pks=sql.SQL(', ').join(pk_ids),
                updates=sql.SQL(', ').join([
                    sql.SQL("{} = EXCLUDED.{}").format(col, col)
                    for col in update_cols
                ])
            )
        else:
            query = sql.SQL("""
                            INSERT INTO {table} ({columns})
                            VALUES ({placeholders})
                            """).format(
                table=table_id,
                columns=column_list,
                placeholders=placeholders
            )

        return query.as_string(self._get_connection())
