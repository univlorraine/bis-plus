"""Tests unitaires pour ECCSourceHook."""
from unittest.mock import MagicMock, patch, mock_open

import pytest


def _make_hook(conn_id='oracle_data'):
    from ecc.hooks.ecc_source_hook import ECCSourceHook
    return ECCSourceHook(conn_id=conn_id)


def _mock_airflow_conn(host='oracle-host', port=1521, schema='SIDSAP', login='user', password='pass'):
    conn = MagicMock()
    conn.host = host
    conn.port = port
    conn.schema = schema
    conn.login = login
    conn.password = password
    return conn


class TestGetOracleDriver:

    def test_returns_oracledb_when_available(self):
        oracle_mod = MagicMock()
        with patch.dict('sys.modules', {'oracledb': oracle_mod}):
            from ecc.hooks import ecc_source_hook
            import importlib
            importlib.reload(ecc_source_hook)
            driver = ecc_source_hook._get_oracle_driver()
        assert driver is oracle_mod

    def test_raises_when_no_driver_available(self):
        with patch.dict('sys.modules', {'oracledb': None, 'cx_Oracle': None}):
            import sys
            sys.modules.pop('oracledb', None)
            sys.modules.pop('cx_Oracle', None)
            from ecc.hooks.ecc_source_hook import _get_oracle_driver
            with pytest.raises(ImportError, match="driver Oracle"):
                # Force both imports to fail
                with patch('builtins.__import__', side_effect=ImportError):
                    _get_oracle_driver()


class TestBuildDsn:

    def test_builds_dsn_with_sid(self):
        hook = _make_hook()
        oracle = MagicMock()
        oracle.makedsn.return_value = '(DESCRIPTION=...)'
        conn = _mock_airflow_conn(host='db-host', port=1521, schema='MYSID')

        result = hook._build_dsn(oracle, conn)

        oracle.makedsn.assert_called_once_with('db-host', 1521, sid='MYSID')
        assert result == '(DESCRIPTION=...)'

    def test_defaults_for_missing_host_port(self):
        hook = _make_hook()
        oracle = MagicMock()
        conn = _mock_airflow_conn(host=None, port=None, schema='SID')

        hook._build_dsn(oracle, conn)

        oracle.makedsn.assert_called_once_with('localhost', 1521, sid='SID')


class TestGetConn:

    def test_connects_with_correct_credentials(self):
        hook = _make_hook()
        oracle = MagicMock()
        airflow_conn = _mock_airflow_conn()

        with patch('ecc.hooks.ecc_source_hook._get_oracle_driver', return_value=oracle), \
             patch('ecc.hooks.ecc_source_hook.get_airflow_connection', return_value=airflow_conn):
            hook.get_conn()

        oracle.connect.assert_called_once()
        call_kwargs = oracle.connect.call_args[1]
        assert call_kwargs['user'] == 'user'
        assert call_kwargs['password'] == 'pass'


class TestExecuteQuery:

    def _setup_oracle(self, rows, columns):
        oracle = MagicMock()
        conn = MagicMock()
        cursor = MagicMock()
        cursor.description = [(col.upper(), None, None, None, None, None, None) for col in columns]
        cursor.fetchmany.side_effect = [rows, []]
        conn.cursor.return_value = cursor
        oracle.connect.return_value = conn
        oracle.makedsn.return_value = 'dsn'
        return oracle, conn, cursor

    def test_returns_column_names_and_generator(self):
        hook = _make_hook()
        oracle, conn, cursor = self._setup_oracle(
            rows=[(1, 'Alice'), (2, 'Bob')],
            columns=['id', 'name']
        )
        airflow_conn = _mock_airflow_conn()

        with patch('ecc.hooks.ecc_source_hook._get_oracle_driver', return_value=oracle), \
             patch('ecc.hooks.ecc_source_hook.get_airflow_connection', return_value=airflow_conn):
            col_names, gen = hook.execute_query("SELECT id, name FROM t")

        assert col_names == ['id', 'name']
        rows = list(gen)
        assert rows == [(1, 'Alice'), (2, 'Bob')]

    def test_closes_connection_after_exhaustion(self):
        hook = _make_hook()
        oracle, conn, cursor = self._setup_oracle(
            rows=[(1,)],
            columns=['id']
        )
        airflow_conn = _mock_airflow_conn()

        with patch('ecc.hooks.ecc_source_hook._get_oracle_driver', return_value=oracle), \
             patch('ecc.hooks.ecc_source_hook.get_airflow_connection', return_value=airflow_conn):
            _, gen = hook.execute_query("SELECT id FROM t")
            list(gen)

        cursor.close.assert_called_once()
        conn.close.assert_called_once()

    def test_closes_connection_on_early_break(self):
        """Generator cleans up even if caller doesn't exhaust it."""
        hook = _make_hook()
        oracle, conn, cursor = self._setup_oracle(
            rows=[(1,), (2,), (3,)],
            columns=['id']
        )
        airflow_conn = _mock_airflow_conn()

        with patch('ecc.hooks.ecc_source_hook._get_oracle_driver', return_value=oracle), \
             patch('ecc.hooks.ecc_source_hook.get_airflow_connection', return_value=airflow_conn):
            _, gen = hook.execute_query("SELECT id FROM t")
            # Only consume one row, then garbage-collect by closing generator
            next(gen)
            gen.close()

        cursor.close.assert_called_once()
        conn.close.assert_called_once()

    def test_raises_on_empty_query(self):
        hook = _make_hook()
        with pytest.raises(ValueError, match="sql_query vide"):
            hook.execute_query("")

    def test_strips_trailing_semicolon(self):
        hook = _make_hook()
        oracle, conn, cursor = self._setup_oracle(rows=[], columns=['id'])
        airflow_conn = _mock_airflow_conn()

        with patch('ecc.hooks.ecc_source_hook._get_oracle_driver', return_value=oracle), \
             patch('ecc.hooks.ecc_source_hook.get_airflow_connection', return_value=airflow_conn):
            hook.execute_query("SELECT 1 FROM dual;")

        executed_sql = cursor.execute.call_args[0][0]
        assert not executed_sql.endswith(';')


class TestExecuteSqlFile:

    def _setup_oracle(self, rows, columns):
        oracle = MagicMock()
        conn = MagicMock()
        cursor = MagicMock()
        cursor.description = [(col.upper(), None, None, None, None, None, None) for col in columns]
        cursor.fetchmany.side_effect = [rows, []]
        conn.cursor.return_value = cursor
        oracle.connect.return_value = conn
        oracle.makedsn.return_value = 'dsn'
        return oracle, conn, cursor

    def test_reads_sql_file_and_executes(self):
        hook = _make_hook()
        oracle, conn, cursor = self._setup_oracle(rows=[(42,)], columns=['val'])
        airflow_conn = _mock_airflow_conn()
        sql_content = "SELECT val FROM table;\n"

        with patch('ecc.hooks.ecc_source_hook._get_oracle_driver', return_value=oracle), \
             patch('ecc.hooks.ecc_source_hook.get_airflow_connection', return_value=airflow_conn), \
             patch('builtins.open', mock_open(read_data=sql_content)):
            col_names, gen = hook.execute_sql_file('/path/to/query.sql')
            rows = list(gen)

        assert col_names == ['val']
        assert rows == [(42,)]
        executed = cursor.execute.call_args[0][0]
        assert not executed.endswith(';')
