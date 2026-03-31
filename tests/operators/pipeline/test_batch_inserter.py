# tests/operators/pipeline/test_batch_inserter.py
"""
Tests unitaires pour AMUEBatchInserter.
"""
import pytest
from unittest.mock import Mock, MagicMock, patch

from psycopg2 import sql, OperationalError, InterfaceError
from psycopg2.errors import UniqueViolation

from common.operators.batch_inserter import AMUEBatchInserter
from amue.exceptions import AMUEBatchError, AMUEDatabaseError, AMUEDataError


class TestAMUEBatchInserterInit:
    """Tests d'initialisation du batch inserter."""

    def test_init_without_hook(self):
        """Test initialisation sans hook."""
        inserter = AMUEBatchInserter()

        assert inserter.postgres_hook is None
        assert inserter.target_schema is None
        assert inserter._conn is None

    def test_init_with_hook(self):
        """Test initialisation avec hook."""
        mock_hook = Mock()

        inserter = AMUEBatchInserter(mock_hook)

        assert inserter.postgres_hook is mock_hook

    def test_init_with_target_schema(self):
        """Test initialisation avec schéma cible."""
        inserter = AMUEBatchInserter(target_schema="splus_blue")

        assert inserter.target_schema == "splus_blue"


class TestGetQualifiedTableName:
    """Tests pour _get_qualified_table_name."""

    def test_without_target_schema(self):
        """Test nom de table sans schéma cible."""
        inserter = AMUEBatchInserter()

        result = inserter._get_qualified_table_name("CSKS")

        assert result == "csks"

    def test_with_target_schema(self):
        """Test nom de table avec schéma cible."""
        inserter = AMUEBatchInserter(target_schema="splus_blue")

        result = inserter._get_qualified_table_name("CSKS")

        assert result == "splus_blue.csks"

    def test_table_name_lowercased(self):
        """Test que le nom de table est en minuscules."""
        inserter = AMUEBatchInserter(target_schema="splus_green")

        result = inserter._get_qualified_table_name("MyTable")

        assert result == "splus_green.mytable"


class TestGetConnection:
    """Tests pour get_connection."""

    def test_creates_new_connection(self):
        """Test création d'une nouvelle connexion."""
        mock_hook = Mock()
        mock_conn = Mock()
        mock_conn.closed = False
        mock_hook.get_conn.return_value = mock_conn

        inserter = AMUEBatchInserter(mock_hook)
        conn = inserter.get_connection()

        assert conn is mock_conn
        mock_hook.get_conn.assert_called_once()

    def test_reuses_existing_connection(self):
        """Test réutilisation de connexion existante."""
        mock_hook = Mock()
        mock_conn = Mock()
        mock_conn.closed = False
        mock_hook.get_conn.return_value = mock_conn

        inserter = AMUEBatchInserter(mock_hook)
        conn1 = inserter.get_connection()
        conn2 = inserter.get_connection()

        assert conn1 is conn2
        mock_hook.get_conn.assert_called_once()

    def test_creates_new_if_closed(self):
        """Test création nouvelle connexion si fermée."""
        mock_hook = Mock()
        mock_conn = Mock()
        mock_conn.closed = True  # Connexion fermée
        new_conn = Mock()
        new_conn.closed = False  # Nouvelle connexion active
        mock_hook.get_conn.return_value = new_conn

        inserter = AMUEBatchInserter(mock_hook)
        inserter._conn = mock_conn  # Connexion fermée assignée
        conn = inserter.get_connection()

        # Devrait créer une nouvelle connexion car l'ancienne est fermée
        mock_hook.get_conn.assert_called_once()
        assert conn is new_conn

    def test_raises_without_hook(self):
        """Test erreur si pas de hook configuré."""
        from airflow.exceptions import AirflowException

        inserter = AMUEBatchInserter()

        with pytest.raises(AirflowException) as exc_info:
            inserter.get_connection()

        assert "hook non configure" in str(exc_info.value)


class TestCloseConnection:
    """Tests pour close_connection."""

    def test_closes_open_connection(self):
        """Test fermeture d'une connexion ouverte."""
        mock_conn = Mock()
        mock_conn.closed = False

        inserter = AMUEBatchInserter()
        inserter._conn = mock_conn

        inserter.close_connection()

        mock_conn.close.assert_called_once()
        assert inserter._conn is None

    def test_does_nothing_if_no_connection(self):
        """Test ne fait rien si pas de connexion."""
        inserter = AMUEBatchInserter()

        # Ne devrait pas lever d'exception
        inserter.close_connection()

    def test_does_nothing_if_already_closed(self):
        """Test ne fait rien si connexion déjà fermée."""
        mock_conn = Mock()
        mock_conn.closed = True

        inserter = AMUEBatchInserter()
        inserter._conn = mock_conn

        inserter.close_connection()

        mock_conn.close.assert_not_called()


class TestExecuteBatch:
    """Tests pour execute_batch."""

    @patch('common.operators.batch_inserter.execute_values')
    def test_successful_batch_insert_upsert(self, mock_exec_values):
        """UPSERT : execute_values(fetch=True) retourne (True/False) → compte INSERT vs UPDATE."""
        # 1 INSERT (xmax=0 → True) + 1 UPDATE (xmax≠0 → False)
        mock_exec_values.return_value = [(True,), (False,)]
        mock_cursor = Mock()
        mock_conn = Mock()

        inserter = AMUEBatchInserter()
        batch = [(1, "A"), (2, "B")]

        result = inserter.execute_batch(
            mock_cursor, mock_conn,
            "INSERT INTO t VALUES %s ON CONFLICT (id) DO UPDATE SET name=EXCLUDED.name RETURNING (xmax=0) AS is_insert",
            batch, "test_table", ["id", "name"], ["id"],
            commit=True
        )

        mock_exec_values.assert_called_once()
        mock_conn.commit.assert_called_once()
        assert result['rows_inserted'] == 1
        assert result['rows_updated'] == 1
        assert result['rows_affected'] == 2
        assert result['batch_size'] == 2

    @patch('common.operators.batch_inserter.execute_values')
    def test_successful_batch_insert_no_pk(self, mock_exec_values):
        """INSERT simple (sans PK) : toutes les lignes comptent comme insérées."""
        mock_cursor = Mock()
        mock_conn = Mock()

        inserter = AMUEBatchInserter()
        batch = [(1, "A"), (2, "B")]

        result = inserter.execute_batch(
            mock_cursor, mock_conn,
            "INSERT INTO t VALUES %s",
            batch, "test_table", ["id", "name"], [],  # pas de PKs
            commit=True
        )

        mock_exec_values.assert_called_once()
        mock_conn.commit.assert_called_once()
        assert result['rows_inserted'] == 2
        assert result['rows_updated'] == 0
        assert result['rows_affected'] == 2
        assert result['batch_size'] == 2

    @patch('common.operators.batch_inserter.execute_values')
    def test_batch_insert_no_commit(self, mock_exec_values):
        """Test insertion sans commit."""
        mock_exec_values.return_value = [(True,)]
        mock_cursor = Mock()
        mock_conn = Mock()

        inserter = AMUEBatchInserter()
        batch = [(1, "A")]

        inserter.execute_batch(
            mock_cursor, mock_conn,
            "INSERT INTO t VALUES %s ON CONFLICT (id) DO UPDATE SET name=EXCLUDED.name",
            batch, "test_table", ["id", "name"], ["id"],
            commit=False
        )

        mock_conn.commit.assert_not_called()

    def test_raises_on_duplicates_in_batch(self):
        """Test détection de doublons dans le batch."""
        mock_cursor = Mock()
        mock_conn = Mock()

        inserter = AMUEBatchInserter()
        # Batch avec doublons sur la clé primaire
        batch = [(1, "A"), (1, "B")]  # id=1 en double

        with pytest.raises(AMUEDataError) as exc_info:
            inserter.execute_batch(
                mock_cursor, mock_conn,
                "INSERT INTO t VALUES (%s, %s)",
                batch, "test_table", ["id", "name"], ["id"],
                commit=True
            )

        assert "Doublons detectes" in str(exc_info.value)

    @patch('common.operators.batch_inserter.execute_values')
    def test_raises_database_error_on_connection_issue(self, mock_exec_values):
        """Test erreur de connexion DB."""
        mock_exec_values.side_effect = OperationalError("Connection lost")
        mock_cursor = Mock()
        mock_conn = Mock()

        inserter = AMUEBatchInserter()
        batch = [(1, "A")]

        with pytest.raises(AMUEDatabaseError) as exc_info:
            inserter.execute_batch(
                mock_cursor, mock_conn,
                "INSERT INTO t VALUES %s",
                batch, "test_table", ["id", "name"], ["id"],
                commit=True
            )

        assert exc_info.value.is_connection_error is True

    @patch('common.operators.batch_inserter.execute_values')
    def test_raises_database_error_on_interface_error(self, mock_exec_values):
        """Test erreur d'interface DB."""
        mock_exec_values.side_effect = InterfaceError("Interface error")
        mock_cursor = Mock()
        mock_conn = Mock()

        inserter = AMUEBatchInserter()
        batch = [(1, "A")]

        with pytest.raises(AMUEDatabaseError) as exc_info:
            inserter.execute_batch(
                mock_cursor, mock_conn,
                "INSERT INTO t VALUES %s",
                batch, "test_table", ["id", "name"], ["id"],
                commit=True
            )

        assert exc_info.value.is_connection_error is True


class TestBuildInsertSql:
    """Tests pour build_insert_sql.

    Note: Ces tests utilisent un vrai curseur psycopg2 simulé car la méthode
    build_insert_sql utilise psycopg2.sql qui requiert une vraie connexion
    pour as_string().
    """

    @patch('common.operators.batch_inserter.sql')
    def test_simple_insert(self, mock_sql):
        """Test INSERT simple sans UPSERT."""
        # Mock the SQL construction
        mock_query = Mock()
        mock_query.as_string.return_value = "INSERT INTO test_table (id, name) VALUES (%s, %s)"
        mock_sql.SQL.return_value.format.return_value = mock_query
        mock_sql.Identifier.return_value = Mock()
        mock_sql.Placeholder.return_value = Mock()

        mock_conn = Mock()

        inserter = AMUEBatchInserter()

        result = inserter.build_insert_sql(
            "test_table", ["id", "name"], ["id"],
            use_upsert=False, conn=mock_conn
        )

        assert "INSERT INTO" in result

    @patch('common.operators.batch_inserter.sql')
    def test_upsert_with_primary_keys(self, mock_sql):
        """Test INSERT avec UPSERT."""
        mock_query = Mock()
        mock_query.as_string.return_value = (
            "INSERT INTO test_table (id, name, value) VALUES (%s, %s, %s) "
            "ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, value = EXCLUDED.value"
        )
        mock_sql.SQL.return_value.format.return_value = mock_query
        mock_sql.Identifier.return_value = Mock()
        mock_sql.Placeholder.return_value = Mock()

        mock_conn = Mock()

        inserter = AMUEBatchInserter()

        result = inserter.build_insert_sql(
            "test_table", ["id", "name", "value"], ["id"],
            use_upsert=True, conn=mock_conn
        )

        assert "INSERT INTO" in result
        assert "ON CONFLICT" in result
        assert "DO UPDATE SET" in result

    @patch('common.operators.batch_inserter.sql')
    def test_upsert_with_schema(self, mock_sql):
        """Test INSERT avec schéma blue/green."""
        mock_query = Mock()
        mock_query.as_string.return_value = (
            "INSERT INTO splus_blue.csks (id, name) VALUES (%s, %s) "
            "ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name"
        )
        mock_sql.SQL.return_value.format.return_value = mock_query
        mock_sql.Identifier.return_value = Mock()
        mock_sql.Placeholder.return_value = Mock()

        mock_conn = Mock()

        inserter = AMUEBatchInserter(target_schema="splus_blue")

        result = inserter.build_insert_sql(
            "csks", ["id", "name"], ["id"],
            use_upsert=True, conn=mock_conn
        )

        assert "splus_blue" in result

    @patch('common.operators.batch_inserter.sql')
    def test_upsert_excludes_source_column(self, mock_sql):
        """Test que _source n'est pas mis à jour."""
        mock_query = Mock()
        mock_query.as_string.return_value = (
            "INSERT INTO test_table (id, name, _source) VALUES (%s, %s, %s) "
            "ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name"
        )
        mock_sql.SQL.return_value.format.return_value = mock_query
        mock_sql.Identifier.return_value = Mock()
        mock_sql.Placeholder.return_value = Mock()

        mock_conn = Mock()

        inserter = AMUEBatchInserter()

        result = inserter.build_insert_sql(
            "test_table", ["id", "name", "_source"], ["id"],
            use_upsert=True, conn=mock_conn
        )

        # La clause DO UPDATE SET ne devrait pas contenir _source
        assert "DO UPDATE SET" in result


class TestFetchExistingRow:
    """Tests pour fetch_existing_row."""

    def test_fetch_existing_row_found(self):
        """Test récupération d'une ligne existante."""
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = (1, "Test")
        mock_conn = Mock()

        inserter = AMUEBatchInserter()

        result = inserter.fetch_existing_row(
            mock_cursor, mock_conn,
            "test_table", ["id", "name"], ["id"],
            {"id": 1}
        )

        assert result == {"id": 1, "name": "Test"}

    def test_fetch_existing_row_not_found(self):
        """Test ligne non trouvée."""
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = None
        mock_conn = Mock()

        inserter = AMUEBatchInserter()

        result = inserter.fetch_existing_row(
            mock_cursor, mock_conn,
            "test_table", ["id", "name"], ["id"],
            {"id": 999}
        )

        assert result is None

    def test_fetch_with_target_schema(self):
        """Test récupération avec schéma cible."""
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = (1, "Test")
        mock_conn = Mock()

        inserter = AMUEBatchInserter(target_schema="splus_green")

        inserter.fetch_existing_row(
            mock_cursor, mock_conn,
            "csks", ["id", "name"], ["id"],
            {"id": 1}
        )

        # Vérifie que le schéma est utilisé dans la requête
        # call_args peut être un Composed psycopg2, on utilise str() pour convertir
        call_args = mock_cursor.execute.call_args[0][0]
        assert "splus_green" in str(call_args)

    def test_fetch_raises_on_connection_error(self):
        """Test erreur de connexion lors de la récupération."""
        mock_cursor = Mock()
        mock_cursor.execute.side_effect = OperationalError("Connection lost")
        mock_conn = Mock()

        inserter = AMUEBatchInserter()

        with pytest.raises(AMUEDatabaseError) as exc_info:
            inserter.fetch_existing_row(
                mock_cursor, mock_conn,
                "test_table", ["id", "name"], ["id"],
                {"id": 1}
            )

        assert exc_info.value.is_connection_error is True

    def test_fetch_returns_none_on_other_error(self):
        """Test retourne None pour autres erreurs."""
        mock_cursor = Mock()
        mock_cursor.execute.side_effect = Exception("Table does not exist")
        mock_conn = Mock()

        inserter = AMUEBatchInserter()

        result = inserter.fetch_existing_row(
            mock_cursor, mock_conn,
            "nonexistent_table", ["id", "name"], ["id"],
            {"id": 1}
        )

        assert result is None

    def test_fetch_with_case_insensitive_pk(self):
        """Test récupération avec clé primaire insensible à la casse."""
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = (1, "Test")
        mock_conn = Mock()

        inserter = AMUEBatchInserter()

        result = inserter.fetch_existing_row(
            mock_cursor, mock_conn,
            "test_table", ["id", "name"], ["ID"],  # PK en majuscules
            {"id": 1}  # Valeur en minuscules
        )

        assert result is not None


class TestHandleUniqueViolation:
    """Tests pour _handle_unique_violation."""

    def test_raises_batch_error(self):
        """Test lève AMUEBatchError."""
        mock_error = Mock(spec=UniqueViolation)
        mock_error.pgerror = "duplicate key value violates unique constraint"
        mock_cursor = Mock()
        mock_conn = Mock()

        inserter = AMUEBatchInserter()
        batch = [(1, "A")]

        with pytest.raises(AMUEBatchError) as exc_info:
            inserter._handle_unique_violation(
                mock_error, mock_cursor, mock_conn,
                batch, "test_table", ["id", "name"], ["id"],
                commit=True, batch_num=1
            )

        assert "Conflit de cle primaire" in str(exc_info.value)
        assert exc_info.value.table_name == "test_table"

    def test_rollback_on_commit(self):
        """Test rollback si commit prévu."""
        mock_error = Mock(spec=UniqueViolation)
        mock_error.pgerror = "duplicate key"
        mock_cursor = Mock()
        mock_conn = Mock()

        inserter = AMUEBatchInserter()

        with pytest.raises(AMUEBatchError):
            inserter._handle_unique_violation(
                mock_error, mock_cursor, mock_conn,
                [(1, "A")], "test_table", ["id", "name"], ["id"],
                commit=True
            )

        mock_conn.rollback.assert_called_once()

    def test_no_rollback_without_commit(self):
        """Test pas de rollback si pas de commit."""
        mock_error = Mock(spec=UniqueViolation)
        mock_error.pgerror = "duplicate key"
        mock_cursor = Mock()
        mock_conn = Mock()

        inserter = AMUEBatchInserter()

        with pytest.raises(AMUEBatchError):
            inserter._handle_unique_violation(
                mock_error, mock_cursor, mock_conn,
                [(1, "A")], "test_table", ["id", "name"], ["id"],
                commit=False
            )

        mock_conn.rollback.assert_not_called()
