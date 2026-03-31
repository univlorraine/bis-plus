"""Tests unitaires pour PostgresConnectionManager."""
from unittest.mock import MagicMock, patch

import pytest


def _make_mock_conn(closed=False):
    conn = MagicMock()
    conn.closed = closed
    return conn


def _make_manager(conn=None):
    from common.utils.database.connection_manager import PostgresConnectionManager
    hook = MagicMock()
    if conn is not None:
        hook.get_conn.return_value = conn
    manager = PostgresConnectionManager(postgres_hook=hook)
    return manager, hook


class TestGetConnection:

    def test_creates_connection_on_first_call(self):
        conn = _make_mock_conn()
        manager, hook = _make_manager(conn)
        result = manager.get_connection()
        assert result is conn
        hook.get_conn.assert_called_once()

    def test_reuses_existing_open_connection(self):
        conn = _make_mock_conn(closed=False)
        manager, hook = _make_manager(conn)
        manager.get_connection()
        manager.get_connection()
        hook.get_conn.assert_called_once()

    def test_creates_new_connection_if_closed(self):
        conn1 = _make_mock_conn(closed=False)
        conn2 = _make_mock_conn(closed=False)
        manager, hook = _make_manager()
        hook.get_conn.side_effect = [conn1, conn2]

        manager.get_connection()
        conn1.closed = True  # simulate connection drop
        manager._conn = conn1
        result = manager.get_connection()
        assert result is conn2
        assert hook.get_conn.call_count == 2

    def test_raises_when_hook_is_none(self):
        from common.utils.database.connection_manager import PostgresConnectionManager
        from airflow.exceptions import AirflowException
        manager = PostgresConnectionManager(postgres_hook=None)
        with pytest.raises(AirflowException):
            manager.get_connection()


class TestClose:

    def test_closes_open_connection(self):
        conn = _make_mock_conn(closed=False)
        manager, _ = _make_manager(conn)
        manager._conn = conn
        manager.close()
        conn.close.assert_called_once()
        assert manager._conn is None

    def test_close_when_no_connection_is_noop(self):
        manager, _ = _make_manager()
        manager.close()  # should not raise

    def test_close_when_already_closed_is_noop(self):
        conn = _make_mock_conn(closed=True)
        manager, _ = _make_manager()
        manager._conn = conn
        manager.close()
        conn.close.assert_not_called()
        assert manager._conn is None

    def test_close_exception_is_swallowed(self):
        conn = _make_mock_conn(closed=False)
        conn.close.side_effect = Exception("unexpected")
        manager, _ = _make_manager()
        manager._conn = conn
        manager.close()  # should not raise
        assert manager._conn is None


class TestRollback:

    def test_rollback_on_open_connection(self):
        conn = _make_mock_conn(closed=False)
        manager, _ = _make_manager()
        manager._conn = conn
        manager.rollback()
        conn.rollback.assert_called_once()

    def test_rollback_when_no_connection_is_noop(self):
        manager, _ = _make_manager()
        manager.rollback()  # should not raise

    def test_rollback_when_closed_is_noop(self):
        conn = _make_mock_conn(closed=True)
        manager, _ = _make_manager()
        manager._conn = conn
        manager.rollback()
        conn.rollback.assert_not_called()

    def test_rollback_exception_is_swallowed(self):
        conn = _make_mock_conn(closed=False)
        conn.rollback.side_effect = Exception("rollback error")
        manager, _ = _make_manager()
        manager._conn = conn
        manager.rollback()  # should not raise


class TestCommit:

    def test_commit_on_open_connection(self):
        conn = _make_mock_conn(closed=False)
        manager, _ = _make_manager()
        manager._conn = conn
        manager.commit()
        conn.commit.assert_called_once()

    def test_commit_without_connection_raises(self):
        from airflow.exceptions import AirflowException
        manager, _ = _make_manager()
        with pytest.raises(AirflowException):
            manager.commit()

    def test_commit_on_closed_connection_raises(self):
        from airflow.exceptions import AirflowException
        conn = _make_mock_conn(closed=True)
        manager, _ = _make_manager()
        manager._conn = conn
        with pytest.raises(AirflowException):
            manager.commit()


class TestIsConnected:

    def test_true_when_connection_open(self):
        conn = _make_mock_conn(closed=False)
        manager, _ = _make_manager()
        manager._conn = conn
        assert manager.is_connected is True

    def test_false_when_no_connection(self):
        manager, _ = _make_manager()
        assert manager.is_connected is False

    def test_false_when_connection_closed(self):
        conn = _make_mock_conn(closed=True)
        manager, _ = _make_manager()
        manager._conn = conn
        assert manager.is_connected is False


class TestContextManager:

    def test_enter_returns_manager(self):
        manager, _ = _make_manager()
        result = manager.__enter__()
        assert result is manager

    def test_exit_without_exception_calls_close(self):
        conn = _make_mock_conn(closed=False)
        manager, _ = _make_manager(conn)
        manager._conn = conn
        with manager:
            pass
        conn.close.assert_called_once()

    def test_exit_with_exception_rollbacks_then_closes(self):
        conn = _make_mock_conn(closed=False)
        manager, _ = _make_manager(conn)
        manager._conn = conn
        try:
            with manager:
                raise ValueError("oops")
        except ValueError:
            pass
        conn.rollback.assert_called_once()
        conn.close.assert_called_once()

    def test_exit_does_not_suppress_exception(self):
        manager, _ = _make_manager()
        with pytest.raises(RuntimeError):
            with manager:
                raise RuntimeError("should propagate")


class TestHookSetter:

    def test_setting_hook_closes_existing_connection(self):
        conn = _make_mock_conn(closed=False)
        manager, _ = _make_manager()
        manager._conn = conn
        new_hook = MagicMock()
        manager.hook = new_hook
        conn.close.assert_called_once()
        assert manager._hook is new_hook

    def test_setting_hook_without_connection_is_safe(self):
        manager, _ = _make_manager()
        new_hook = MagicMock()
        manager.hook = new_hook  # should not raise
        assert manager._hook is new_hook
