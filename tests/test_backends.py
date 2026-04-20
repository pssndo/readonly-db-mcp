"""Unit tests for database backend utilities — identifier validation, LIMIT injection, DSN construction, guards.

These tests don't require a running database. They test the shared utilities
and constructor logic that can be verified without network access.
"""

import pytest

from readonly_db_mcp.databases.base import validate_identifier, inject_limit
from readonly_db_mcp.databases.postgres import PostgresBackend
from readonly_db_mcp.databases.clickhouse import (
    ClickHouseBackend,
    _is_retriable_connection_error,
)
from readonly_db_mcp.databases.mysql import MySQLBackend
from readonly_db_mcp.config import (
    PostgresConnection,
    ClickHouseConnection,
    MysqlConnection,
    MariaDBConnection,
    Config,
)


# ---------------------------------------------------------------------------
# validate_identifier — shared by both backends
# ---------------------------------------------------------------------------


class TestValidateIdentifier:
    """Tests for the SQL identifier validation function."""

    def test_simple_name(self) -> None:
        assert validate_identifier("users") == "users"

    def test_schema_dot_table(self) -> None:
        assert validate_identifier("public.users") == "public.users"

    def test_underscore_prefix(self) -> None:
        assert validate_identifier("_internal_table") == "_internal_table"

    def test_alphanumeric_with_underscores(self) -> None:
        assert validate_identifier("my_table_v2") == "my_table_v2"

    def test_clickhouse_db_dot_table(self) -> None:
        assert validate_identifier("analytics.events") == "analytics.events"

    def test_leading_digit_rejected(self) -> None:
        with pytest.raises(ValueError, match="Invalid identifier"):
            validate_identifier("123abc")

    def test_space_rejected(self) -> None:
        with pytest.raises(ValueError, match="Invalid identifier"):
            validate_identifier("my table")

    def test_semicolon_injection_rejected(self) -> None:
        with pytest.raises(ValueError, match="Invalid identifier"):
            validate_identifier("users; DROP TABLE users")

    def test_quote_rejected(self) -> None:
        with pytest.raises(ValueError, match="Invalid identifier"):
            validate_identifier("users'--")

    def test_empty_string_rejected(self) -> None:
        with pytest.raises(ValueError, match="Invalid identifier"):
            validate_identifier("")

    def test_dash_rejected(self) -> None:
        with pytest.raises(ValueError, match="Invalid identifier"):
            validate_identifier("my-table")

    def test_parentheses_rejected(self) -> None:
        with pytest.raises(ValueError, match="Invalid identifier"):
            validate_identifier("table()")

    def test_backtick_rejected(self) -> None:
        with pytest.raises(ValueError, match="Invalid identifier"):
            validate_identifier("`users`")

    def test_double_dot_passes_regex(self) -> None:
        """Double dots like 'public..users' pass the regex — this is fine
        because the database will reject it as an invalid table name.
        The regex is a safety net, not a full SQL parser."""
        # This passes the regex — the DB will catch it
        assert validate_identifier("public..users") == "public..users"


# ---------------------------------------------------------------------------
# PostgresBackend — constructor and guards (no real DB needed)
# ---------------------------------------------------------------------------


class TestPostgresBackendUnit:
    """Unit tests for PostgresBackend that don't need a running database."""

    def _make_pg_config(self, **overrides) -> PostgresConnection:
        defaults = {
            "name": "test",
            "host": "localhost",
            "port": 5432,
            "database": "testdb",
            "user": "reader",
            "password": "secret",
        }
        defaults.update(overrides)
        return PostgresConnection(**defaults)

    def test_ensure_connected_raises_before_connect(self) -> None:
        backend = PostgresBackend(self._make_pg_config())
        with pytest.raises(RuntimeError, match="Not connected"):
            backend._ensure_connected()

    def test_constructor_stores_config(self) -> None:
        config = Config(
            postgres_connections=[], clickhouse_connections=[], query_timeout_seconds=60, max_result_rows=500
        )
        backend = PostgresBackend(self._make_pg_config(), config)
        assert backend._timeout == 60
        assert backend._max_rows == 500
        assert backend.host == "localhost"
        assert backend.database == "testdb"
        assert backend.name == "test"

    def test_constructor_defaults_without_config(self) -> None:
        backend = PostgresBackend(self._make_pg_config())
        assert backend._timeout == 30
        assert backend._max_rows == 1000

    def test_dsn_special_chars_in_password(self) -> None:
        """Verify that special characters in passwords are handled.
        We can't test the full connect() without a DB, but we can verify
        the URL-encoding logic by inspecting the code path."""
        backend = PostgresBackend(self._make_pg_config(password="p@ss:w/rd#100%"))
        # The password is stored raw; encoding happens in connect()
        assert backend.password == "p@ss:w/rd#100%"


# ---------------------------------------------------------------------------
# ClickHouseBackend — constructor and guards (no real DB needed)
# ---------------------------------------------------------------------------


class TestClickHouseBackendUnit:
    """Unit tests for ClickHouseBackend that don't need a running database."""

    def _make_ch_config(self, **overrides) -> ClickHouseConnection:
        defaults = {
            "name": "test",
            "host": "localhost",
            "port": 8123,
            "database": "testdb",
            "user": "reader",
            "password": "secret",
        }
        defaults.update(overrides)
        return ClickHouseConnection(**defaults)

    def test_ensure_connected_raises_before_connect(self) -> None:
        backend = ClickHouseBackend(self._make_ch_config())
        with pytest.raises(RuntimeError, match="Not connected"):
            backend._ensure_connected()

    def test_constructor_stores_config(self) -> None:
        config = Config(
            postgres_connections=[], clickhouse_connections=[], query_timeout_seconds=60, max_result_rows=500
        )
        backend = ClickHouseBackend(self._make_ch_config(), config)
        assert backend._timeout == 60
        assert backend._max_rows == 500
        assert backend.host == "localhost"
        assert backend.database == "testdb"
        assert backend.name == "test"

    def test_constructor_defaults_without_config(self) -> None:
        backend = ClickHouseBackend(self._make_ch_config())
        assert backend._timeout == 30
        assert backend._max_rows == 1000


class TestClickHouseRetryClassification:
    """Tests for classifying retriable vs non-retriable ClickHouse errors."""

    def test_connection_error_is_retriable(self) -> None:
        error = RuntimeError("Connection reset by peer")
        assert _is_retriable_connection_error(error)

    def test_network_error_is_retriable(self) -> None:
        error = RuntimeError("network error while reading response")
        assert _is_retriable_connection_error(error)

    def test_query_timeout_is_not_retriable(self) -> None:
        error = RuntimeError("DB::Exception: Time limit exceeded")
        assert not _is_retriable_connection_error(error)

    def test_max_execution_time_is_not_retriable(self) -> None:
        error = RuntimeError("DB::Exception: max_execution_time exceeded")
        assert not _is_retriable_connection_error(error)


# ---------------------------------------------------------------------------
# MySQLBackend — constructor and flavor-specific behavior (no real DB needed)
# ---------------------------------------------------------------------------


class TestMySQLBackendUnit:
    """Unit tests for MySQLBackend that don't need a running database.

    The same backend class serves MySQL and MariaDB; the `flavor` parameter
    selects which timeout session variable is emitted per-query.
    """

    def _make_mysql_config(self, **overrides) -> MysqlConnection:
        defaults = {
            "name": "test_mysql",
            "host": "localhost",
            "port": 3306,
            "database": "testdb",
            "user": "reader",
            "password": "secret",
        }
        defaults.update(overrides)
        return MysqlConnection(**defaults)

    def _make_mariadb_config(self, **overrides) -> MariaDBConnection:
        defaults = {
            "name": "test_mariadb",
            "host": "localhost",
            "port": 3306,
            "database": "testdb",
            "user": "reader",
            "password": "secret",
        }
        defaults.update(overrides)
        return MariaDBConnection(**defaults)

    def test_ensure_connected_raises_before_connect(self) -> None:
        backend = MySQLBackend(self._make_mysql_config(), flavor="mysql")
        with pytest.raises(RuntimeError, match="Not connected"):
            backend._ensure_connected()

    def test_mysql_flavor_db_type(self) -> None:
        backend = MySQLBackend(self._make_mysql_config(), flavor="mysql")
        assert backend.db_type == "mysql"
        assert backend.flavor == "mysql"

    def test_mariadb_flavor_db_type(self) -> None:
        backend = MySQLBackend(self._make_mariadb_config(), flavor="mariadb")
        assert backend.db_type == "mariadb"
        assert backend.flavor == "mariadb"

    def test_mysql_timeout_prelude_uses_max_execution_time_in_ms(self) -> None:
        """MySQL's max_execution_time is in milliseconds (SELECT-only scope)."""
        config = Config(
            postgres_connections=[], clickhouse_connections=[],
            query_timeout_seconds=30, max_result_rows=1000,
        )
        backend = MySQLBackend(self._make_mysql_config(), config, flavor="mysql")
        prelude = backend._timeout_prelude_sql()
        assert "max_execution_time" in prelude
        assert "30000" in prelude  # 30 seconds * 1000 = 30000 ms
        # Don't leak the MariaDB variant
        assert "max_statement_time" not in prelude

    def test_mariadb_timeout_prelude_uses_max_statement_time_in_seconds(self) -> None:
        """MariaDB's max_statement_time is in seconds (applies to all statements)."""
        config = Config(
            postgres_connections=[], clickhouse_connections=[],
            query_timeout_seconds=30, max_result_rows=1000,
        )
        backend = MySQLBackend(self._make_mariadb_config(), config, flavor="mariadb")
        prelude = backend._timeout_prelude_sql()
        assert "max_statement_time" in prelude
        assert " = 30" in prelude  # 30 seconds, not ms
        # Don't leak the MySQL variant
        assert "max_execution_time" not in prelude

    def test_constructor_stores_config(self) -> None:
        config = Config(
            postgres_connections=[], clickhouse_connections=[],
            query_timeout_seconds=60, max_result_rows=500,
        )
        backend = MySQLBackend(self._make_mysql_config(), config, flavor="mysql")
        assert backend._timeout == 60
        assert backend._max_rows == 500
        assert backend.host == "localhost"
        assert backend.database == "testdb"
        assert backend.name == "test_mysql"

    def test_constructor_defaults_without_config(self) -> None:
        backend = MySQLBackend(self._make_mysql_config(), flavor="mysql")
        assert backend._timeout == 30
        assert backend._max_rows == 1000


# ---------------------------------------------------------------------------
# inject_limit — shared LIMIT injection for both backends
# ---------------------------------------------------------------------------


class TestInjectLimit:
    """Tests for the inject_limit function that adds/tightens LIMIT clauses."""

    def test_adds_limit_to_simple_select(self) -> None:
        result = inject_limit("SELECT * FROM users", 100, "postgres")
        assert "LIMIT" in result.upper()
        assert "100" in result

    def test_preserves_order_by(self) -> None:
        """ORDER BY must be preserved — this was the main reason for switching
        from subquery wrapping to AST-based LIMIT injection."""
        result = inject_limit("SELECT * FROM users ORDER BY name", 100, "postgres")
        assert "ORDER BY" in result.upper()
        assert "100" in result

    def test_tightens_high_limit(self) -> None:
        """If user's LIMIT is higher than max_rows, replace with max_rows."""
        result = inject_limit("SELECT * FROM users LIMIT 5000", 100, "postgres")
        assert "100" in result
        assert "5000" not in result

    def test_keeps_low_limit(self) -> None:
        """If user's LIMIT is already within bounds, keep it unchanged."""
        result = inject_limit("SELECT * FROM users LIMIT 10", 100, "postgres")
        assert "10" in result

    def test_union_gets_limit(self) -> None:
        result = inject_limit("SELECT * FROM t1 UNION ALL SELECT * FROM t2", 100, "postgres")
        assert "LIMIT" in result.upper()

    def test_clickhouse_dialect(self) -> None:
        result = inject_limit("SELECT count() FROM events", 100, "clickhouse")
        assert "LIMIT" in result.upper()

    def test_cte_query(self) -> None:
        sql = "WITH cte AS (SELECT * FROM users) SELECT * FROM cte ORDER BY id"
        result = inject_limit(sql, 100, "postgres")
        assert "LIMIT" in result.upper()
        assert "ORDER BY" in result.upper()

    def test_order_by_with_existing_limit(self) -> None:
        """ORDER BY + LIMIT already present and within bounds — both preserved."""
        result = inject_limit("SELECT * FROM users ORDER BY name LIMIT 50", 100, "postgres")
        assert "ORDER BY" in result.upper()
        assert "50" in result
