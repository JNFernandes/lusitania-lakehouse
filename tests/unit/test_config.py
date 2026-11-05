"""The SQL Server connection string: Windows login by default, SQL login when SQL_USER is set."""
import importlib

import pytest

from lusitania import config


@pytest.fixture
def reload_config(monkeypatch):
    """config reads the environment when it is imported: reload it after changing the variables."""
    def _reload(**env):
        for name in ("SQL_USER", "SQL_PASSWORD", "SQL_SERVER"):
            monkeypatch.delenv(name, raising=False)
        for name, value in env.items():
            monkeypatch.setenv(name, value)
        return importlib.reload(config)
    yield _reload
    monkeypatch.undo()
    importlib.reload(config)


def test_windows_authentication_by_default(reload_config):
    cfg = reload_config(SQL_SERVER="localhost")
    conn = cfg.sql_connection_string()
    assert "Trusted_Connection=yes;" in conn
    assert "UID=" not in conn


def test_sql_login_when_user_is_set(reload_config):
    cfg = reload_config(SQL_SERVER="host.docker.internal,1433", SQL_USER="lusitania_etl", SQL_PASSWORD="secret")
    conn = cfg.sql_connection_string()
    assert "SERVER=host.docker.internal,1433;" in conn
    assert "UID=lusitania_etl;PWD=secret;" in conn
    assert "Trusted_Connection" not in conn
