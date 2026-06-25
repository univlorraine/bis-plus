"""Tests unitaires pour ECCSourceHook (Oracle + SQL Server)."""
from unittest.mock import MagicMock, patch, mock_open

import pytest


def _make_hook(conn_id='ecc_data'):
    from ecc.infrastructure.hooks.ecc_source_hook import ECCSourceHook
    return ECCSourceHook(conn_id=conn_id)


def _mock_airflow_conn(
    host='oracle-host',
    port=1521,
    schema='SIDSAP',
    login='user',
    password='pass',
    conn_type='oracle',
    extra=None,
):
    conn = MagicMock()
    conn.host = host
    conn.port = port
    conn.schema = schema
    conn.login = login
    conn.password = password
    conn.conn_type = conn_type
    conn.extra = extra
    return conn


# ─────────────────────────────────────────────────────────────────────────────
# _resolve_backend
# ─────────────────────────────────────────────────────────────────────────────
class TestResolveBackend:

    def test_oracle_conn_type(self):
        hook = _make_hook()
        assert hook._resolve_backend(_mock_airflow_conn(conn_type='oracle')) == 'oracle'

    def test_mssql_conn_type(self):
        hook = _make_hook()
        assert hook._resolve_backend(_mock_airflow_conn(conn_type='mssql')) == 'mssql'

    def test_mssqlplus_conn_type(self):
        hook = _make_hook()
        assert hook._resolve_backend(_mock_airflow_conn(conn_type='mssqlplus')) == 'mssql'

    def test_odbc_defaults_to_oracle(self):
        """Rétro-compat : connexion ODBC sans extra.backend → Oracle."""
        hook = _make_hook()
        assert hook._resolve_backend(_mock_airflow_conn(conn_type='odbc')) == 'oracle'

    def test_odbc_with_extra_backend_mssql(self):
        hook = _make_hook()
        conn = _mock_airflow_conn(conn_type='odbc', extra='{"backend": "mssql"}')
        assert hook._resolve_backend(conn) == 'mssql'

    def test_odbc_with_extra_backend_dict(self):
        hook = _make_hook()
        conn = _mock_airflow_conn(conn_type='odbc', extra={'backend': 'mssql'})
        assert hook._resolve_backend(conn) == 'mssql'

    def test_odbc_with_invalid_backend(self):
        hook = _make_hook()
        conn = _mock_airflow_conn(conn_type='odbc', extra='{"backend": "postgres"}')
        with pytest.raises(ValueError, match="extra.backend"):
            hook._resolve_backend(conn)

    def test_unsupported_conn_type(self):
        hook = _make_hook()
        with pytest.raises(ValueError, match="conn_type"):
            hook._resolve_backend(_mock_airflow_conn(conn_type='postgres'))


# ─────────────────────────────────────────────────────────────────────────────
# _get_driver
# ─────────────────────────────────────────────────────────────────────────────
class TestGetDriver:

    def test_returns_oracledb_when_available(self):
        from ecc.infrastructure.hooks.ecc_source_hook import _get_driver
        oracle_mod = MagicMock()
        with patch.dict('sys.modules', {'oracledb': oracle_mod}):
            driver = _get_driver('oracle')
        assert driver is oracle_mod

    def test_returns_pyodbc_for_mssql(self):
        from ecc.infrastructure.hooks.ecc_source_hook import _get_driver
        pyodbc_mod = MagicMock()
        with patch.dict('sys.modules', {'pyodbc': pyodbc_mod}):
            driver = _get_driver('mssql')
        assert driver is pyodbc_mod

    def test_raises_on_unknown_backend(self):
        from ecc.infrastructure.hooks.ecc_source_hook import _get_driver
        with pytest.raises(ValueError, match="Backend non supporté"):
            _get_driver('postgres')

    def test_raises_when_pyodbc_missing(self):
        from ecc.infrastructure.hooks.ecc_source_hook import _get_driver
        with patch.dict('sys.modules', {'pyodbc': None}):
            import sys
            sys.modules.pop('pyodbc', None)
            with patch('builtins.__import__', side_effect=ImportError):
                with pytest.raises(ImportError, match="pyodbc"):
                    _get_driver('mssql')


# ─────────────────────────────────────────────────────────────────────────────
# _build_connect_kwargs : Oracle
# ─────────────────────────────────────────────────────────────────────────────
class TestOracleConnectKwargs:

    def test_builds_dsn_with_sid(self):
        hook = _make_hook()
        oracle = MagicMock()
        oracle.makedsn.return_value = '(DESCRIPTION=...)'
        conn = _mock_airflow_conn(
            host='db-host', port=1521, schema='MYSID', conn_type='oracle'
        )

        kwargs = hook._build_connect_kwargs('oracle', oracle, conn)

        oracle.makedsn.assert_called_once_with('db-host', 1521, sid='MYSID')
        assert kwargs == {
            'user': 'user',
            'password': 'pass',
            'dsn': '(DESCRIPTION=...)',
            'expire_time': 2,
        }

    def test_defaults_for_missing_host_port(self):
        hook = _make_hook()
        oracle = MagicMock()
        conn = _mock_airflow_conn(
            host=None, port=None, schema='SID', conn_type='oracle'
        )

        hook._build_connect_kwargs('oracle', oracle, conn)

        oracle.makedsn.assert_called_once_with('localhost', 1521, sid='SID')


# ─────────────────────────────────────────────────────────────────────────────
# _build_connect_kwargs : SQL Server
# ─────────────────────────────────────────────────────────────────────────────
class TestMSSQLConnectKwargs:

    def test_builds_odbc_dsn(self):
        hook = _make_hook()
        conn = _mock_airflow_conn(
            host='sqlsrv-host',
            port=1433,
            schema='MABASE',
            login='myuser',
            password='mypass',
            conn_type='mssql',
        )

        kwargs = hook._build_connect_kwargs('mssql', MagicMock(), conn)

        dsn = kwargs['dsn']
        assert 'Driver={ODBC Driver 17 for SQL Server}' in dsn
        assert 'Server=sqlsrv-host,1433' in dsn
        assert 'Database=MABASE' in dsn
        assert 'UID=myuser' in dsn
        assert 'PWD=mypass' in dsn
        assert 'Encrypt=yes' in dsn

    def test_default_port_when_missing(self):
        hook = _make_hook()
        conn = _mock_airflow_conn(
            host='h', port=None, schema='db', conn_type='mssql'
        )

        kwargs = hook._build_connect_kwargs('mssql', MagicMock(), conn)
        assert 'Server=h,1433' in kwargs['dsn']

    def test_custom_driver_via_extra(self):
        hook = _make_hook()
        conn = _mock_airflow_conn(
            host='h', port=1433, schema='db', conn_type='mssql',
            extra='{"driver": "ODBC Driver 18 for SQL Server"}',
        )

        kwargs = hook._build_connect_kwargs('mssql', MagicMock(), conn)
        assert 'Driver={ODBC Driver 18 for SQL Server}' in kwargs['dsn']


# ─────────────────────────────────────────────────────────────────────────────
# get_conn
# ─────────────────────────────────────────────────────────────────────────────
class TestGetConnOracle:

    def test_connects_with_correct_credentials(self):
        hook = _make_hook()
        oracle = MagicMock()
        airflow_conn = _mock_airflow_conn(conn_type='oracle')

        with patch('ecc.infrastructure.hooks.ecc_source_hook._get_driver', return_value=oracle), \
             patch('ecc.infrastructure.hooks.ecc_source_hook.get_airflow_connection', return_value=airflow_conn):
            hook.get_conn()

        oracle.connect.assert_called_once()
        call_kwargs = oracle.connect.call_args[1]
        assert call_kwargs['user'] == 'user'
        assert call_kwargs['password'] == 'pass'
        assert call_kwargs['expire_time'] == 2


class TestGetConnMSSQL:

    def test_connects_with_dsn_string(self):
        hook = _make_hook()
        pyodbc = MagicMock()
        airflow_conn = _mock_airflow_conn(
            host='sqlsrv', port=1433, schema='DB', conn_type='mssql'
        )

        with patch('ecc.infrastructure.hooks.ecc_source_hook._get_driver', return_value=pyodbc), \
             patch('ecc.infrastructure.hooks.ecc_source_hook.get_airflow_connection', return_value=airflow_conn):
            hook.get_conn()

        pyodbc.connect.assert_called_once()
        # pyodbc.connect prend une chaîne unique (un seul positional arg)
        args, kwargs = pyodbc.connect.call_args
        assert len(args) == 1
        assert kwargs == {}
        assert 'Server=sqlsrv,1433' in args[0]
        assert 'Database=DB' in args[0]


# ─────────────────────────────────────────────────────────────────────────────
# _is_connection_error
# ─────────────────────────────────────────────────────────────────────────────
class TestIsConnectionErrorOracle:

    def test_detects_dpy_4011(self):
        from ecc.infrastructure.hooks.ecc_source_hook import ECCSourceHook
        assert ECCSourceHook._is_connection_error(Exception('DPY-4011: ...'), 'oracle')

    def test_detects_connection_was_closed(self):
        from ecc.infrastructure.hooks.ecc_source_hook import ECCSourceHook
        assert ECCSourceHook._is_connection_error(
            Exception('the connection was closed'), 'oracle'
        )

    def test_does_not_match_unrelated_error(self):
        from ecc.infrastructure.hooks.ecc_source_hook import ECCSourceHook
        assert not ECCSourceHook._is_connection_error(
            Exception('ORA-00942: table or view does not exist'), 'oracle'
        )


class TestIsConnectionErrorMSSQL:

    def test_detects_08s01(self):
        from ecc.infrastructure.hooks.ecc_source_hook import ECCSourceHook
        assert ECCSourceHook._is_connection_error(
            Exception("[08S01] Communication link failure"), 'mssql'
        )

    def test_detects_08001(self):
        from ecc.infrastructure.hooks.ecc_source_hook import ECCSourceHook
        assert ECCSourceHook._is_connection_error(
            Exception("[08001] Cannot connect"), 'mssql'
        )

    def test_does_not_match_42s02(self):
        from ecc.infrastructure.hooks.ecc_source_hook import ECCSourceHook
        assert not ECCSourceHook._is_connection_error(
            Exception("[42S02] Invalid object name"), 'mssql'
        )


# ─────────────────────────────────────────────────────────────────────────────
# execute_query : Oracle
# ─────────────────────────────────────────────────────────────────────────────
class TestExecuteQueryOracle:

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
            columns=['id', 'name'],
        )
        airflow_conn = _mock_airflow_conn(conn_type='oracle')

        with patch('ecc.infrastructure.hooks.ecc_source_hook._get_driver', return_value=oracle), \
             patch('ecc.infrastructure.hooks.ecc_source_hook.get_airflow_connection', return_value=airflow_conn):
            col_names, gen = hook.execute_query("SELECT id, name FROM t")
            rows = list(gen)

        assert col_names == ['id', 'name']
        assert rows == [(1, 'Alice'), (2, 'Bob')]

    def test_closes_connection_after_exhaustion(self):
        hook = _make_hook()
        oracle, conn, cursor = self._setup_oracle(rows=[(1,)], columns=['id'])
        airflow_conn = _mock_airflow_conn(conn_type='oracle')

        with patch('ecc.infrastructure.hooks.ecc_source_hook._get_driver', return_value=oracle), \
             patch('ecc.infrastructure.hooks.ecc_source_hook.get_airflow_connection', return_value=airflow_conn):
            _, gen = hook.execute_query("SELECT id FROM t")
            list(gen)

        cursor.close.assert_called_once()
        conn.close.assert_called_once()

    def test_closes_connection_on_early_break(self):
        hook = _make_hook()
        oracle, conn, cursor = self._setup_oracle(
            rows=[(1,), (2,), (3,)], columns=['id']
        )
        airflow_conn = _mock_airflow_conn(conn_type='oracle')

        with patch('ecc.infrastructure.hooks.ecc_source_hook._get_driver', return_value=oracle), \
             patch('ecc.infrastructure.hooks.ecc_source_hook.get_airflow_connection', return_value=airflow_conn):
            _, gen = hook.execute_query("SELECT id FROM t")
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
        airflow_conn = _mock_airflow_conn(conn_type='oracle')

        with patch('ecc.infrastructure.hooks.ecc_source_hook._get_driver', return_value=oracle), \
             patch('ecc.infrastructure.hooks.ecc_source_hook.get_airflow_connection', return_value=airflow_conn):
            hook.execute_query("SELECT 1 FROM dual;")

        executed_sql = cursor.execute.call_args[0][0]
        assert not executed_sql.endswith(';')


# ─────────────────────────────────────────────────────────────────────────────
# execute_query : SQL Server
# ─────────────────────────────────────────────────────────────────────────────
class TestExecuteQueryMSSQL:

    def _setup_pyodbc(self, rows, columns):
        pyodbc = MagicMock()
        conn = MagicMock()
        cursor = MagicMock()
        cursor.description = [(col.upper(), None, None, None, None, None, None) for col in columns]
        cursor.fetchmany.side_effect = [rows, []]
        conn.cursor.return_value = cursor
        pyodbc.connect.return_value = conn
        return pyodbc, conn, cursor

    def test_streams_rows_from_mssql(self):
        hook = _make_hook()
        pyodbc, conn, cursor = self._setup_pyodbc(
            rows=[(1, 'Alice'), (2, 'Bob')], columns=['id', 'name']
        )
        airflow_conn = _mock_airflow_conn(
            host='sqlsrv', port=1433, schema='DB', conn_type='mssql'
        )

        with patch('ecc.infrastructure.hooks.ecc_source_hook._get_driver', return_value=pyodbc), \
             patch('ecc.infrastructure.hooks.ecc_source_hook.get_airflow_connection', return_value=airflow_conn):
            col_names, gen = hook.execute_query("SELECT id, name FROM t")
            rows = list(gen)

        assert col_names == ['id', 'name']
        assert rows == [(1, 'Alice'), (2, 'Bob')]
        # pyodbc.connect appelé avec une chaîne unique (positional)
        args, _ = pyodbc.connect.call_args
        assert len(args) == 1
        assert 'Server=sqlsrv,1433' in args[0]

    def test_closes_connection_after_exhaustion(self):
        hook = _make_hook()
        pyodbc, conn, cursor = self._setup_pyodbc(rows=[(1,)], columns=['id'])
        airflow_conn = _mock_airflow_conn(
            host='sqlsrv', port=1433, schema='DB', conn_type='mssql'
        )

        with patch('ecc.infrastructure.hooks.ecc_source_hook._get_driver', return_value=pyodbc), \
             patch('ecc.infrastructure.hooks.ecc_source_hook.get_airflow_connection', return_value=airflow_conn):
            _, gen = hook.execute_query("SELECT id FROM t")
            list(gen)

        cursor.close.assert_called_once()
        conn.close.assert_called_once()


# ─────────────────────────────────────────────────────────────────────────────
# execute_sql_file
# ─────────────────────────────────────────────────────────────────────────────
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
        airflow_conn = _mock_airflow_conn(conn_type='oracle')
        sql_content = "SELECT val FROM table;\n"

        with patch('ecc.infrastructure.hooks.ecc_source_hook._get_driver', return_value=oracle), \
             patch('ecc.infrastructure.hooks.ecc_source_hook.get_airflow_connection', return_value=airflow_conn), \
             patch('builtins.open', mock_open(read_data=sql_content)):
            col_names, gen = hook.execute_sql_file('/path/to/query.sql')
            rows = list(gen)

        assert col_names == ['val']
        assert rows == [(42,)]
        executed = cursor.execute.call_args[0][0]
        assert not executed.endswith(';')
