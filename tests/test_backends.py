"""Unit tests for database backend utilities — identifier validation, LIMIT injection, DSN construction, guards.

These tests don't require a running database (except the SQLite round-trip
suite which uses a temp file). They test the shared utilities and constructor
logic that can be verified without network access.
"""

import asyncio
import time

import pytest

from readonly_db_mcp.databases.base import validate_identifier, inject_limit
from readonly_db_mcp.databases.postgres import PostgresBackend
from readonly_db_mcp.databases.clickhouse import (
    ClickHouseBackend,
    _is_retriable_connection_error,
)
from readonly_db_mcp.databases.mysql import MariaDBBackend, MySQLBackend
from readonly_db_mcp.databases.sqlite import SqliteBackend
from readonly_db_mcp.config import (
    PostgresConnection,
    ClickHouseConnection,
    MysqlConnection,
    MariaDBConnection,
    SqliteConnection,
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
    """Unit tests for MySQLBackend / MariaDBBackend (no real DB needed).

    MySQL and MariaDB share an implementation via a common base class
    (`_MySQLFamilyBackend`), but each concrete subclass declares its own
    class-level `db_type` attribute — matching the PG/CH pattern. The
    `db_type` drives timeout-SQL selection at runtime, so the two behaviors
    are pinned at class level rather than passed as a parameter.
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
        backend = MySQLBackend(self._make_mysql_config())
        with pytest.raises(RuntimeError, match="Not connected"):
            backend._ensure_connected()

    def test_mysql_db_type_is_class_level(self) -> None:
        # db_type is declared on the class itself — matches postgres.py /
        # clickhouse.py pattern, no per-instance shadowing.
        assert MySQLBackend.db_type == "mysql"
        backend = MySQLBackend(self._make_mysql_config())
        assert backend.db_type == "mysql"

    def test_mariadb_db_type_is_class_level(self) -> None:
        assert MariaDBBackend.db_type == "mariadb"
        backend = MariaDBBackend(self._make_mariadb_config())
        assert backend.db_type == "mariadb"

    def test_mysql_timeout_prelude_uses_max_execution_time_in_ms(self) -> None:
        """MySQL's max_execution_time is in milliseconds (SELECT-only scope)."""
        config = Config(
            postgres_connections=[], clickhouse_connections=[],
            query_timeout_seconds=30, max_result_rows=1000,
        )
        backend = MySQLBackend(self._make_mysql_config(), config)
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
        backend = MariaDBBackend(self._make_mariadb_config(), config)
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
        backend = MySQLBackend(self._make_mysql_config(), config)
        assert backend._timeout == 60
        assert backend._max_rows == 500
        assert backend.host == "localhost"
        assert backend.database == "testdb"
        assert backend.name == "test_mysql"

    def test_constructor_defaults_without_config(self) -> None:
        backend = MySQLBackend(self._make_mysql_config())
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


# ---------------------------------------------------------------------------
# SqliteBackend — unit tests + real-file round-trip (SQLite is stdlib, so we
# can exercise the full stack without a separate server process).
# ---------------------------------------------------------------------------


class TestSqliteBackendUnit:
    """Constructor + guard tests that don't need a real file."""

    def _make_config(self, **overrides) -> SqliteConnection:
        defaults = {"name": "test_sqlite", "path": "/tmp/does_not_exist.db"}
        defaults.update(overrides)
        return SqliteConnection(**defaults)

    def test_ensure_connected_raises_before_connect(self) -> None:
        backend = SqliteBackend(self._make_config())
        with pytest.raises(RuntimeError, match="Not connected"):
            backend._ensure_connected()

    def test_constructor_stores_config(self) -> None:
        app_cfg = Config(
            postgres_connections=[], clickhouse_connections=[],
            query_timeout_seconds=45, max_result_rows=250,
        )
        backend = SqliteBackend(self._make_config(path="/data/app.db"), app_cfg)
        assert backend._timeout == 45
        assert backend._max_rows == 250
        assert backend.path == "/data/app.db"
        # host/database are synthetic for list_databases uniformity
        assert backend.host == "(file)"
        assert backend.database == "/data/app.db"

    def test_db_type_is_class_level(self) -> None:
        assert SqliteBackend.db_type == "sqlite"

    def test_constructor_defaults_without_config(self) -> None:
        backend = SqliteBackend(self._make_config())
        assert backend._timeout == 30
        assert backend._max_rows == 1000

    def test_path_with_question_mark_rejected(self) -> None:
        """'?' in path would confuse the SQLite URI parser (which uses '?' to
        separate path from query string). Reject at open time with a clear
        error rather than passing through to an unexpected URI interpretation."""
        import asyncio
        backend = SqliteBackend(self._make_config(path="/tmp/weird?name.db"))
        with pytest.raises(ValueError, match="conflicts with URI syntax"):
            asyncio.run(backend.connect())


class TestSqliteBackendRoundTrip:
    """Real SQLite round-trip tests. SQLite is stdlib, so we can set up a
    temp DB file and exercise the full backend without any external service.

    These tests are the only place in the suite where we actually run SQL
    against a live DB, which makes them especially valuable for the SQLite
    backend. They exercise: VFS-level read-only (can't write), ATTACH
    blocking, extension-loading block, list_tables, describe_table, execute,
    explain.
    """

    async def _fresh_db(self, tmp_path) -> SqliteBackend:
        """Create a temp SQLite DB, populate it, and return a connected backend."""
        import sqlite3
        db_path = tmp_path / "test.db"
        # Populate with a normal (writable) connection, then open the
        # backend separately in read-only mode.
        conn = sqlite3.connect(str(db_path))
        conn.executescript("""
            CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT);
            INSERT INTO users (name, email) VALUES ('Alice', 'a@x'), ('Bob', NULL);
            CREATE TABLE orders (id INTEGER, user_id INTEGER, total REAL);
        """)
        conn.commit()
        conn.close()

        backend = SqliteBackend(SqliteConnection(name="test", path=str(db_path)))
        await backend.connect()
        return backend

    async def test_readonly_mode_blocks_writes(self, tmp_path) -> None:
        """Core security test: even if the validator were bypassed, the
        VFS-level read-only mode refuses writes at the SQLite layer."""
        backend = await self._fresh_db(tmp_path)
        try:
            # Bypass the validator by calling backend.execute directly with
            # raw SQL. SQLite itself must still refuse the write.
            import sqlite3
            with pytest.raises(sqlite3.OperationalError, match="readonly|read-only"):
                await backend.execute("INSERT INTO users (name) VALUES ('Charlie')")
        finally:
            await backend.disconnect()

    async def test_attach_database_not_blocked_at_vfs_layer(self, tmp_path) -> None:
        """IMPORTANT: ATTACH DATABASE is NOT blocked by VFS read-only mode.
        SQLite will happily ATTACH another file (also read-only) to a
        read-only main DB. This means the validator (Layer 1) is our only
        defense against ATTACH — if it were ever relaxed, a caller could
        attach paths outside the operator's configured set.

        This test documents that fact so a future change doesn't accidentally
        start relying on VFS read-only to block ATTACH."""
        backend = await self._fresh_db(tmp_path)
        try:
            other_path = tmp_path / "other.db"
            import sqlite3
            sqlite3.connect(str(other_path)).close()
            # This succeeds — SQLite attaches the other file read-only.
            # The test is here to catch regressions in our understanding of
            # SQLite's behavior, not to assert a block that doesn't exist.
            await backend.execute(f"ATTACH DATABASE '{other_path}' AS other")
        finally:
            await backend.disconnect()

    async def test_list_tables_excludes_sqlite_internals(self, tmp_path) -> None:
        backend = await self._fresh_db(tmp_path)
        try:
            tables = await backend.list_tables()
            assert "users" in tables
            assert "orders" in tables
            # sqlite_* internal tables are excluded
            assert not any(t.startswith("sqlite_") for t in tables)
        finally:
            await backend.disconnect()

    async def test_list_tables_with_schema_param_raises(self, tmp_path) -> None:
        backend = await self._fresh_db(tmp_path)
        try:
            with pytest.raises(ValueError, match="does not support the `schema` parameter"):
                await backend.list_tables(schema="main")
        finally:
            await backend.disconnect()

    async def test_describe_table(self, tmp_path) -> None:
        backend = await self._fresh_db(tmp_path)
        try:
            cols = await backend.describe_table("users")
            names = [c["name"] for c in cols]
            assert names == ["id", "name", "email"]
            # NOT NULL enforcement is reflected in `nullable`
            by_name = {c["name"]: c for c in cols}
            assert by_name["name"]["nullable"] == "NO"
            assert by_name["email"]["nullable"] == "YES"
        finally:
            await backend.disconnect()

    async def test_describe_table_rejects_unsafe_identifier(self, tmp_path) -> None:
        backend = await self._fresh_db(tmp_path)
        try:
            with pytest.raises(ValueError, match="Invalid identifier"):
                await backend.describe_table("users; DROP TABLE users")
        finally:
            await backend.disconnect()

    async def test_execute_returns_rows(self, tmp_path) -> None:
        backend = await self._fresh_db(tmp_path)
        try:
            cols, rows, total = await backend.execute("SELECT id, name FROM users ORDER BY id")
            assert cols == ["id", "name"]
            assert rows == [(1, "Alice"), (2, "Bob")]
            assert total == 2
        finally:
            await backend.disconnect()

    async def test_execute_zero_rows_preserves_columns(self, tmp_path) -> None:
        """DB-API 2.0 guarantees cursor.description is populated even for
        zero-row results, so empty queries still carry the schema."""
        backend = await self._fresh_db(tmp_path)
        try:
            cols, rows, total = await backend.execute(
                "SELECT id, name FROM users WHERE id = -999"
            )
            assert cols == ["id", "name"]
            assert rows == []
            assert total == 0
        finally:
            await backend.disconnect()

    async def test_explain_query_plan_is_plan_only(self, tmp_path) -> None:
        """EXPLAIN QUERY PLAN is strictly plan-only in SQLite — no execution.
        We verify it returns something human-readable rather than raw opcodes."""
        backend = await self._fresh_db(tmp_path)
        try:
            plan = await backend.explain("SELECT * FROM users WHERE id = 1")
            # Should mention the table in the plan (SEARCH or SCAN users)
            assert "users" in plan.lower()
        finally:
            await backend.disconnect()

    async def test_load_extension_is_disabled(self, tmp_path) -> None:
        """load_extension() would be a write bypass — the backend disables it
        at connect time. Verify it raises when called."""
        backend = await self._fresh_db(tmp_path)
        try:
            import sqlite3
            # load_extension raises with a clear message when disabled.
            # We call it via the raw connection to test the backend's
            # connect-time hardening, not via execute (which would also be
            # blocked by the validator at the tool layer).
            conn = backend._ensure_connected()
            with pytest.raises(sqlite3.OperationalError, match="not authorized|disabled"):
                conn.execute("SELECT load_extension('/tmp/nonexistent.so')")
        finally:
            await backend.disconnect()

    async def test_enable_load_extension_not_supported_is_swallowed(self, tmp_path, monkeypatch) -> None:
        """On Python builds without SQLITE_ENABLE_LOAD_EXTENSION (e.g. some
        Debian/Ubuntu system Python installs), `enable_load_extension` raises
        NotSupportedError. That's fine — extension loading is already absent
        from the build. The backend must start successfully on those builds.

        We can't patch `sqlite3.Connection.enable_load_extension` directly —
        it's a C-type method and the attribute is read-only. Instead we wrap
        the connection in a pass-through proxy whose own
        `enable_load_extension` raises, and patch `sqlite3.connect` at module
        scope to return the proxy. The backend calls `sqlite3.connect` by
        module attribute, so the monkeypatch takes effect at call time.
        """
        import sqlite3

        db_path = tmp_path / "restricted.db"
        sqlite3.connect(str(db_path)).close()  # create empty DB
        real_connect = sqlite3.connect

        class _RestrictedConn:
            """Proxy that raises NotSupportedError from enable_load_extension
            but delegates everything else to a real sqlite3.Connection."""
            def __init__(self, *args, **kwargs):
                self._real = real_connect(*args, **kwargs)

            def enable_load_extension(self, enabled: bool) -> None:
                raise sqlite3.NotSupportedError(
                    "SQLite was compiled without SQLITE_ENABLE_LOAD_EXTENSION"
                )

            def __getattr__(self, name):
                return getattr(self._real, name)

        monkeypatch.setattr(sqlite3, "connect", _RestrictedConn)

        backend = SqliteBackend(SqliteConnection(name="r", path=str(db_path)))
        await backend.connect()  # must not raise
        try:
            # And it must remain usable — list_tables uses the wrapped conn.
            tables = await backend.list_tables()
            assert isinstance(tables, list)
        finally:
            await backend.disconnect()

    async def test_execution_timeout_aborts_long_query(self, tmp_path) -> None:
        """The progress handler enforces _timeout as a real execution timeout
        (not just a lock-wait). A deliberately slow self-join should be
        aborted and surface as sqlite3.OperationalError('interrupted').

        We use a self-join on a sizeable table instead of a recursive CTE
        because sqlglot's SQLite dialect has trouble round-tripping the
        `WITH RECURSIVE c(x) AS (...)` syntax (column names in a CTE alias).
        The self-join approach also exercises more VDBE opcodes per unit of
        work, which matches real slow-query shapes.
        """
        import sqlite3

        db_path = tmp_path / "t.db"
        # Populate with enough rows that a 3-way self-join runs >1s. On a
        # modern CPU, 500 rows × 500 × 500 = 125M row pairs examined.
        conn = sqlite3.connect(str(db_path))
        conn.executescript(
            "CREATE TABLE big (i INTEGER);"
            "WITH RECURSIVE s AS (SELECT 1 AS x UNION ALL SELECT x+1 FROM s WHERE x < 500) "
            "INSERT INTO big SELECT x FROM s;"
        )
        conn.commit()
        conn.close()

        # Configure a 1-second execution timeout
        cfg = Config(
            postgres_connections=[], clickhouse_connections=[],
            query_timeout_seconds=1, max_result_rows=1000,
        )
        backend = SqliteBackend(SqliteConnection(name="t", path=str(db_path)), cfg)
        await backend.connect()
        try:
            # Cartesian 3-way self-join. The progress handler fires every
            # 1000 VDBE ops and aborts once the 1s deadline passes.
            slow_query = "SELECT count(*) FROM big a, big b, big c WHERE a.i < b.i AND b.i < c.i"
            start = time.monotonic()
            with pytest.raises(sqlite3.OperationalError, match="interrupted"):
                await backend.execute(slow_query)
            elapsed = time.monotonic() - start
            # Should abort within a few seconds of the 1s deadline. Generous
            # upper bound to avoid flakiness on slow CI.
            assert elapsed < 10, f"execution timeout took {elapsed}s, should be ~1s"
        finally:
            await backend.disconnect()

    async def test_concurrent_queries_serialize_safely(self, tmp_path) -> None:
        """Two concurrent execute() calls against the same backend share one
        sqlite3.Connection. Without serialization they would race on cursor
        state / SQLITE_MISUSE. With the asyncio.Lock, both complete correctly."""
        backend = await self._fresh_db(tmp_path)
        try:
            # Launch two queries concurrently. Both should return their own
            # independent result sets.
            results = await asyncio.gather(
                backend.execute("SELECT id, name FROM users ORDER BY id"),
                backend.execute("SELECT id, name FROM users ORDER BY id DESC"),
            )
            cols_asc, rows_asc, _ = results[0]
            cols_desc, rows_desc, _ = results[1]
            assert cols_asc == ["id", "name"]
            assert cols_desc == ["id", "name"]
            assert rows_asc == [(1, "Alice"), (2, "Bob")]
            assert rows_desc == [(2, "Bob"), (1, "Alice")]
        finally:
            await backend.disconnect()

    async def test_disconnect_waits_for_inflight_query(self, tmp_path) -> None:
        """disconnect() must acquire the same lock as queries so it can't
        close the connection while a query is still using it on a worker
        thread. We verify by launching a slow query, sleeping briefly to let
        it start, then calling disconnect — the disconnect should wait for
        the query to finish and then close cleanly."""
        # Use a timeout long enough that the query would complete normally.
        cfg = Config(
            postgres_connections=[], clickhouse_connections=[],
            query_timeout_seconds=30, max_result_rows=1000,
        )

        import sqlite3
        db_path = tmp_path / "t.db"
        # Populate with enough rows that the query takes perceptible time.
        conn = sqlite3.connect(str(db_path))
        conn.executescript(
            "CREATE TABLE t (i INTEGER);"
            "WITH RECURSIVE s AS (SELECT 1 AS x UNION ALL SELECT x+1 FROM s WHERE x < 300) "
            "INSERT INTO t SELECT x FROM s;"
        )
        conn.commit()
        conn.close()

        backend = SqliteBackend(SqliteConnection(name="t", path=str(db_path)), cfg)
        await backend.connect()

        # Launch a self-join query that will take a measurable moment.
        query_task = asyncio.create_task(
            backend.execute("SELECT count(*) FROM t a, t b WHERE a.i <= b.i")
        )
        # Let the query actually start (grab the lock).
        await asyncio.sleep(0.1)

        # Now call disconnect. It must wait for the query to finish rather
        # than closing the connection while execute() is mid-flight.
        await backend.disconnect()

        # The query should have completed cleanly before disconnect returned.
        cols, rows, _ = await query_task
        # sqlglot's round-trip may upcase the function name in the column
        # label (count(*) → COUNT(*)); compare case-insensitively.
        assert len(cols) == 1 and cols[0].lower() == "count(*)"
        assert rows[0][0] > 0

    async def test_query_after_disconnect_raises_cleanly(self, tmp_path) -> None:
        """If a query is queued behind disconnect (gets the lock after it),
        it should see a clean RuntimeError('Not connected') rather than
        crashing inside sqlite3 on a closed connection."""
        backend = await self._fresh_db(tmp_path)
        await backend.disconnect()

        # Now try to run a query against the disconnected backend.
        with pytest.raises(RuntimeError, match="Not connected"):
            await backend.execute("SELECT 1")
