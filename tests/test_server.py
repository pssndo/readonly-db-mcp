"""Tests for MCP server tool functions."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from readonly_db_mcp.config import Config
from readonly_db_mcp.server import (
    AppContext,
    _get_backend,
    query_postgres,
    query_clickhouse,
    query_mysql,
    query_mariadb,
    query_sqlite,
    list_databases,
    list_tables,
    describe_table,
    explain_query,
    sample_table,
    usage_guide,
)


def _make_ctx(backends: dict | None = None, config: Config | None = None) -> MagicMock:
    """Create a mock MCP Context with the given backends."""
    if config is None:
        config = Config(postgres_connections=[], clickhouse_connections=[], max_result_rows=1000)
    app = AppContext(backends=backends or {}, config=config)
    ctx = MagicMock()
    ctx.request_context.lifespan_context = app
    return ctx


def _make_pg_backend(name: str = "testpg") -> MagicMock:
    """Create a mock PostgreSQL backend."""
    backend = MagicMock()
    backend.db_type = "postgres"
    backend.host = "pghost"
    backend.database = "pgdb"
    backend.name = name
    backend.execute = AsyncMock(return_value=(["id", "name"], [(1, "Alice"), (2, "Bob")], 2))
    backend.list_tables = AsyncMock(return_value=["public.users", "public.orders"])
    backend.describe_table = AsyncMock(
        return_value=[
            {"name": "id", "type": "integer", "nullable": "NO"},
            {"name": "name", "type": "text", "nullable": "YES"},
        ]
    )
    backend.table_stats = AsyncMock(return_value=None)  # PG backend returns no stats
    backend.explain = AsyncMock(return_value="Seq Scan on users  (cost=0.00..1.00 rows=1 width=36)")
    return backend


def _make_ch_backend(name: str = "testch") -> MagicMock:
    """Create a mock ClickHouse backend."""
    backend = MagicMock()
    backend.db_type = "clickhouse"
    backend.host = "chhost"
    backend.database = "chdb"
    backend.name = name
    backend.execute = AsyncMock(return_value=(["count()"], [(42,)], 1))
    backend.list_tables = AsyncMock(return_value=["events", "sessions"])
    backend.describe_table = AsyncMock(
        return_value=[
            {"name": "event_id", "type": "UInt64", "nullable": "NO"},
            {"name": "payload", "type": "Nullable(String)", "nullable": "YES"},
        ]
    )
    backend.table_stats = AsyncMock(return_value=None)  # can be overridden per-test
    backend.explain = AsyncMock(return_value="Expression\n  ReadFromMergeTree")
    return backend


def _make_sqlite_backend(name: str = "testsqlite") -> MagicMock:
    """Create a mock SQLite backend."""
    backend = MagicMock()
    backend.db_type = "sqlite"
    backend.host = "(file)"
    backend.database = "/tmp/fake.db"
    backend.path = "/tmp/fake.db"
    backend.name = name
    backend.execute = AsyncMock(return_value=(["id", "name"], [(1, "Alice"), (2, "Bob")], 2))
    backend.list_tables = AsyncMock(return_value=["users", "orders"])
    backend.describe_table = AsyncMock(
        return_value=[
            {"name": "id", "type": "INTEGER", "nullable": "NO"},
            {"name": "name", "type": "TEXT", "nullable": "YES"},
        ]
    )
    backend.table_stats = AsyncMock(return_value=None)
    backend.explain = AsyncMock(return_value="SEARCH users USING INTEGER PRIMARY KEY (id=?)")
    return backend


def _make_mysql_backend(name: str = "testmysql", db_type: str = "mysql") -> MagicMock:
    """Create a mock MySQL/MariaDB backend.

    The real classes are `MySQLBackend` and `MariaDBBackend` (each with its
    own class-level db_type), but from the server's perspective the only
    contract is the abstract DatabaseBackend method set + the db_type attr.
    Pass db_type="mariadb" to exercise MariaDB routing in _get_backend.
    """
    backend = MagicMock()
    backend.db_type = db_type
    backend.host = "myhost"
    backend.database = "mydb"
    backend.name = name
    backend.execute = AsyncMock(return_value=(["id", "name"], [(1, "Alice"), (2, "Bob")], 2))
    backend.list_tables = AsyncMock(return_value=["users", "orders"])
    backend.describe_table = AsyncMock(
        return_value=[
            {"name": "id", "type": "int unsigned", "nullable": "NO"},
            {"name": "name", "type": "varchar(255)", "nullable": "YES"},
        ]
    )
    backend.table_stats = AsyncMock(return_value=None)
    backend.explain = AsyncMock(return_value="  id: 1\n  select_type: SIMPLE\n  table: users")
    return backend


class TestGetBackend:
    """Tests for backend resolution."""

    def test_get_by_name(self) -> None:
        pg = _make_pg_backend()
        ctx = _make_ctx({"testpg": pg})
        assert _get_backend(ctx, "testpg") is pg

    def test_get_by_name_unknown_raises(self) -> None:
        ctx = _make_ctx({"testpg": _make_pg_backend()})
        with pytest.raises(ValueError, match="Unknown database"):
            _get_backend(ctx, "nonexistent")

    def test_get_first_by_type(self) -> None:
        pg = _make_pg_backend()
        ch = _make_ch_backend()
        ctx = _make_ctx({"testpg": pg, "testch": ch})
        assert _get_backend(ctx, None, db_type="clickhouse") is ch

    def test_get_no_matching_type_raises(self) -> None:
        ctx = _make_ctx({"testpg": _make_pg_backend()})
        with pytest.raises(ValueError, match="No clickhouse database configured"):
            _get_backend(ctx, None, db_type="clickhouse")


class TestQueryPostgres:
    """Tests for query_postgres tool."""

    async def test_valid_select(self) -> None:
        pg = _make_pg_backend()
        ctx = _make_ctx({"testpg": pg})
        result = await query_postgres("SELECT * FROM users", ctx)
        assert "| id | name |" in result
        assert "| 1 | Alice |" in result
        pg.execute.assert_called_once_with("SELECT * FROM users")

    async def test_write_query_rejected(self) -> None:
        ctx = _make_ctx({"testpg": _make_pg_backend()})
        result = await query_postgres("DROP TABLE users", ctx)
        assert "Error:" in result

    async def test_no_pg_configured(self) -> None:
        ctx = _make_ctx({"testch": _make_ch_backend()})
        result = await query_postgres("SELECT 1", ctx)
        assert "Error:" in result

    async def test_named_database(self) -> None:
        pg = _make_pg_backend()
        ctx = _make_ctx({"mypg": pg})
        result = await query_postgres("SELECT 1", ctx, database="mypg")
        assert "Error" not in result

    async def test_trailing_semicolon_uses_cleaned_sql(self) -> None:
        pg = _make_pg_backend()
        ctx = _make_ctx({"testpg": pg})
        await query_postgres("SELECT 1;", ctx)
        pg.execute.assert_called_once_with("SELECT 1")


class TestQueryClickhouse:
    """Tests for query_clickhouse tool."""

    async def test_valid_select(self) -> None:
        ch = _make_ch_backend()
        ctx = _make_ctx({"testch": ch})
        result = await query_clickhouse("SELECT count() FROM events", ctx)
        assert "| count() |" in result
        assert "| 42 |" in result

    async def test_write_query_rejected(self) -> None:
        ctx = _make_ctx({"testch": _make_ch_backend()})
        result = await query_clickhouse("INSERT INTO events VALUES (1)", ctx)
        assert "Error:" in result

    async def test_trailing_semicolon_uses_cleaned_sql(self) -> None:
        ch = _make_ch_backend()
        ctx = _make_ctx({"testch": ch})
        await query_clickhouse("SELECT count() FROM events;", ctx)
        ch.execute.assert_called_once_with("SELECT count() FROM events")


class TestListDatabases:
    """Tests for list_databases tool."""

    async def test_lists_all(self) -> None:
        ctx = _make_ctx({"testpg": _make_pg_backend(), "testch": _make_ch_backend()})
        result = await list_databases(ctx)
        assert "**testpg** (postgres)" in result
        assert "**testch** (clickhouse)" in result

    async def test_empty(self) -> None:
        ctx = _make_ctx({})
        result = await list_databases(ctx)
        assert result == "No databases configured."


class TestListTables:
    """Tests for list_tables tool."""

    async def test_pg_tables(self) -> None:
        ctx = _make_ctx({"testpg": _make_pg_backend()})
        result = await list_tables("testpg", ctx)
        assert "public.users" in result
        assert "public.orders" in result

    async def test_unknown_db(self) -> None:
        ctx = _make_ctx({})
        result = await list_tables("nope", ctx)
        assert "Error:" in result

    async def test_schema_param_forwarded_to_backend(self) -> None:
        """When a schema is given, the backend's list_tables should be called
        with schema=... so the backend can scope its metadata query."""
        pg = _make_pg_backend()
        ctx = _make_ctx({"testpg": pg})
        await list_tables("testpg", ctx, schema="reporting")
        pg.list_tables.assert_called_once_with(schema="reporting")

    async def test_no_schema_passes_none_to_backend(self) -> None:
        """Without a schema, the backend gets schema=None — default scope."""
        pg = _make_pg_backend()
        ctx = _make_ctx({"testpg": pg})
        await list_tables("testpg", ctx)
        pg.list_tables.assert_called_once_with(schema=None)

    async def test_schema_identifier_validated(self) -> None:
        """Unsafe schema names must be rejected before reaching the backend.
        Even though backends parameterize the value, defense-in-depth: catch
        obviously-bad input here with a clear error."""
        pg = _make_pg_backend()
        ctx = _make_ctx({"testpg": pg})
        result = await list_tables("testpg", ctx, schema="foo; DROP TABLE bar")
        assert "Error:" in result
        assert "Invalid identifier" in result
        pg.list_tables.assert_not_called()

    async def test_empty_result_for_schema_mentions_schema(self) -> None:
        pg = _make_pg_backend()
        pg.list_tables = AsyncMock(return_value=[])
        ctx = _make_ctx({"testpg": pg})
        result = await list_tables("testpg", ctx, schema="empty_schema")
        assert "empty_schema" in result
        assert "No tables found" in result


class TestDescribeTable:
    """Tests for describe_table tool."""

    async def test_pg_describe(self) -> None:
        ctx = _make_ctx({"testpg": _make_pg_backend()})
        result = await describe_table("testpg", "public.users", ctx)
        assert "| column | type | nullable |" in result
        assert "| id | integer | NO |" in result
        assert "| name | text | YES |" in result


class TestExplainQuery:
    """Tests for explain_query tool."""

    async def test_explain_pg(self) -> None:
        pg = _make_pg_backend()
        ctx = _make_ctx({"testpg": pg})
        result = await explain_query("SELECT * FROM users", "testpg", ctx)
        assert "Seq Scan" in result

    async def test_explain_rejects_write(self) -> None:
        ctx = _make_ctx({"testpg": _make_pg_backend()})
        result = await explain_query("DROP TABLE users", "testpg", ctx)
        assert "Error:" in result

    async def test_explain_trailing_semicolon_uses_cleaned_sql(self) -> None:
        pg = _make_pg_backend()
        ctx = _make_ctx({"testpg": pg})
        await explain_query("SELECT * FROM users;", "testpg", ctx)
        pg.explain.assert_called_once_with("SELECT * FROM users", analyze=False)


class TestDescribeTableWithStats:
    """describe_table should append backend-provided table metadata when available."""

    async def test_ch_describe_includes_stats(self) -> None:
        ch = _make_ch_backend()
        ch.table_stats = AsyncMock(
            return_value={
                "engine": "MergeTree",
                "total_rows": 1000,
                "total_bytes": 2048,
                "primary_key": "id",
                "sorting_key": "id",
                "partition_key": "",
            }
        )
        ctx = _make_ctx({"testch": ch})
        result = await describe_table("testch", "events", ctx)
        assert "| event_id | UInt64 | NO |" in result
        assert "**Table metadata:**" in result
        assert "engine: MergeTree" in result
        assert "total_rows: 1000" in result
        # Empty partition_key should be suppressed
        assert "partition_key:" not in result

    async def test_pg_describe_has_no_stats_section(self) -> None:
        # PG backend returns None from table_stats, so no metadata section
        ctx = _make_ctx({"testpg": _make_pg_backend()})
        result = await describe_table("testpg", "public.users", ctx)
        assert "Table metadata" not in result


class TestSampleTable:
    """sample_table should issue a validated SELECT * LIMIT n."""

    async def test_sample_basic(self) -> None:
        pg = _make_pg_backend()
        ctx = _make_ctx({"testpg": pg}, config=Config(postgres_connections=[], clickhouse_connections=[], max_result_rows=1000))
        result = await sample_table("testpg", "public.users", ctx, n=5)
        pg.execute.assert_called_once()
        sent_sql = pg.execute.call_args[0][0]
        assert "SELECT * FROM public.users LIMIT 5" == sent_sql
        assert "| id | name |" in result

    async def test_sample_rejects_unsafe_identifier(self) -> None:
        ctx = _make_ctx({"testpg": _make_pg_backend()})
        result = await sample_table("testpg", "users; DROP TABLE x", ctx)
        assert "Error:" in result
        assert "Invalid identifier" in result

    async def test_sample_rejects_n_over_max(self) -> None:
        ctx = _make_ctx({"testpg": _make_pg_backend()}, config=Config(postgres_connections=[], clickhouse_connections=[], max_result_rows=100))
        result = await sample_table("testpg", "public.users", ctx, n=5000)
        assert "Error:" in result
        assert "MAX_RESULT_ROWS" in result

    async def test_sample_rejects_n_below_one(self) -> None:
        ctx = _make_ctx({"testpg": _make_pg_backend()})
        result = await sample_table("testpg", "public.users", ctx, n=0)
        assert "Error:" in result

    async def test_sample_rejects_invalid_format(self) -> None:
        ctx = _make_ctx({"testpg": _make_pg_backend()})
        result = await sample_table("testpg", "public.users", ctx, output_format="xml")
        assert "Error:" in result
        assert "output_format" in result


class TestUsageGuide:
    """usage_guide should describe the configured connections and tool surface."""

    async def test_guide_lists_connections_and_tools(self) -> None:
        ctx = _make_ctx({"testpg": _make_pg_backend(), "testch": _make_ch_backend()})
        result = await usage_guide(ctx)
        assert "testpg" in result
        assert "testch" in result
        # Each major tool name mentioned
        for tool in ("list_databases", "list_tables", "describe_table", "sample_table",
                     "query_postgres", "query_clickhouse", "explain_query"):
            assert tool in result
        # Documents the three output formats
        assert "vertical" in result
        assert "json" in result


class TestQueryOutputFormats:
    """query_* tools should honor the output_format param — tested for both backends
    to prevent format regressions slipping in via one path but not the other."""

    # ── PostgreSQL ───────────────────────────────────────────────────────

    async def test_pg_json_format(self) -> None:
        import json as _json
        ctx = _make_ctx({"testpg": _make_pg_backend()})
        result = await query_postgres("SELECT * FROM users", ctx, output_format="json")
        # JSON output is a strict envelope — always valid JSON, always same shape.
        data = _json.loads(result)
        assert data["columns"] == ["id", "name"]
        assert data["rows"] == [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        assert data["truncated"] is False

    async def test_pg_vertical_format(self) -> None:
        ctx = _make_ctx({"testpg": _make_pg_backend()})
        result = await query_postgres("SELECT * FROM users", ctx, output_format="vertical")
        assert "-[ RECORD 1 ]-" in result

    async def test_pg_invalid_format(self) -> None:
        ctx = _make_ctx({"testpg": _make_pg_backend()})
        result = await query_postgres("SELECT * FROM users", ctx, output_format="xml")
        assert "Error:" in result

    # ── ClickHouse (parity) ──────────────────────────────────────────────

    async def test_ch_json_format(self) -> None:
        import json as _json
        ctx = _make_ctx({"testch": _make_ch_backend()})
        result = await query_clickhouse("SELECT count() FROM events", ctx, output_format="json")
        data = _json.loads(result)
        assert data["columns"] == ["count()"]
        assert data["rows"] == [{"count()": 42}]
        assert data["truncated"] is False

    async def test_ch_vertical_format(self) -> None:
        ctx = _make_ctx({"testch": _make_ch_backend()})
        result = await query_clickhouse("SELECT count() FROM events", ctx, output_format="vertical")
        assert "-[ RECORD 1 ]-" in result
        assert "count() = 42" in result

    async def test_ch_invalid_format(self) -> None:
        ctx = _make_ctx({"testch": _make_ch_backend()})
        result = await query_clickhouse("SELECT 1", ctx, output_format="xml")
        assert "Error:" in result
        assert "output_format" in result

    async def test_ch_table_format_default(self) -> None:
        """Default format is markdown table for ClickHouse too."""
        ctx = _make_ctx({"testch": _make_ch_backend()})
        result = await query_clickhouse("SELECT count() FROM events", ctx)
        assert "| count() |" in result
        assert "| 42 |" in result


class TestErrorForwarding:
    """query_* tools should forward the DB driver's error message, no stack traces."""

    async def test_forwards_error_message(self) -> None:
        pg = _make_pg_backend()
        pg.execute = AsyncMock(side_effect=RuntimeError('relation "foo" does not exist'))
        ctx = _make_ctx({"testpg": pg})
        result = await query_postgres("SELECT * FROM foo", ctx)
        assert "relation \"foo\" does not exist" in result
        # Never a traceback
        assert "Traceback" not in result

    async def test_error_message_capped(self) -> None:
        pg = _make_pg_backend()
        pg.execute = AsyncMock(side_effect=RuntimeError("x" * 2000))
        ctx = _make_ctx({"testpg": pg})
        result = await query_postgres("SELECT 1", ctx)
        # Message truncated at 500 chars
        assert "x" * 600 not in result
        assert "..." in result


class TestSchemaErrorHint:
    """Schema-shaped driver errors (unknown column, unknown table, etc.) should
    suffix a hint pointing the AI at describe_table / list_tables.

    The hint never replaces the driver's message — it's appended. Driver
    messages that don't look schema-related pass through unchanged."""

    async def test_mysql_unknown_column_gets_hint(self) -> None:
        my = _make_mysql_backend()
        my.execute = AsyncMock(
            side_effect=RuntimeError("(1054, \"Unknown column 'foo' in 'field list'\")")
        )
        ctx = _make_ctx({"testmysql": my})
        result = await query_mysql("SELECT foo FROM users", ctx)
        # Driver's message is preserved
        assert "Unknown column" in result
        # Hint points at the right tools
        assert "describe_table" in result
        assert "list_tables" in result

    async def test_postgres_column_does_not_exist_gets_hint(self) -> None:
        pg = _make_pg_backend()
        pg.execute = AsyncMock(
            side_effect=RuntimeError('column "foo" does not exist')
        )
        ctx = _make_ctx({"testpg": pg})
        result = await query_postgres("SELECT foo FROM users", ctx)
        assert "does not exist" in result
        assert "describe_table" in result

    async def test_clickhouse_unknown_identifier_gets_hint(self) -> None:
        ch = _make_ch_backend()
        ch.execute = AsyncMock(
            side_effect=RuntimeError("Code: 47. DB::Exception: Unknown identifier: foo")
        )
        ctx = _make_ctx({"testch": ch})
        result = await query_clickhouse("SELECT foo FROM events", ctx)
        assert "Unknown identifier" in result
        assert "describe_table" in result

    async def test_generic_error_does_not_get_hint(self) -> None:
        """A timeout or connection error shouldn't suggest describe_table —
        that would mislead the AI."""
        pg = _make_pg_backend()
        pg.execute = AsyncMock(side_effect=RuntimeError("connection refused"))
        ctx = _make_ctx({"testpg": pg})
        result = await query_postgres("SELECT 1", ctx)
        assert "connection refused" in result
        assert "describe_table" not in result

    async def test_explain_query_unknown_column_gets_hint(self) -> None:
        """User's reported scenario: EXPLAIN of a query with a bad column name.
        The hint should show up on the explain path too."""
        pg = _make_pg_backend()
        pg.explain = AsyncMock(
            side_effect=RuntimeError('column "typo" does not exist')
        )
        ctx = _make_ctx({"testpg": pg})
        result = await explain_query("SELECT typo FROM users", "testpg", ctx)
        assert "does not exist" in result
        assert "describe_table" in result


# ---------------------------------------------------------------------------
# MySQL / MariaDB tool paths
# ---------------------------------------------------------------------------


class TestQueryMysql:
    """Tests for query_mysql tool — mirrors TestQueryPostgres parity."""

    async def test_valid_select(self) -> None:
        my = _make_mysql_backend()
        ctx = _make_ctx({"testmysql": my})
        result = await query_mysql("SELECT * FROM users", ctx)
        assert "| id | name |" in result
        assert "| 1 | Alice |" in result
        my.execute.assert_called_once_with("SELECT * FROM users")

    async def test_write_query_rejected(self) -> None:
        ctx = _make_ctx({"testmysql": _make_mysql_backend()})
        result = await query_mysql("DROP TABLE users", ctx)
        assert "Error:" in result

    async def test_show_tables_rejected_with_hint(self) -> None:
        ctx = _make_ctx({"testmysql": _make_mysql_backend()})
        result = await query_mysql("SHOW TABLES", ctx)
        assert "Error:" in result
        assert "list_tables" in result

    async def test_backtick_identifiers_accepted(self) -> None:
        my = _make_mysql_backend()
        ctx = _make_ctx({"testmysql": my})
        result = await query_mysql("SELECT `id`, `name` FROM `users`", ctx)
        assert "Error" not in result

    async def test_no_mysql_configured(self) -> None:
        # Only a postgres backend — query_mysql should fail to find one
        ctx = _make_ctx({"testpg": _make_pg_backend()})
        result = await query_mysql("SELECT 1", ctx)
        assert "Error:" in result
        assert "No mysql database configured" in result

    async def test_json_output(self) -> None:
        import json as _json
        ctx = _make_ctx({"testmysql": _make_mysql_backend()})
        result = await query_mysql("SELECT * FROM users", ctx, output_format="json")
        data = _json.loads(result)
        assert data["columns"] == ["id", "name"]
        assert data["rows"] == [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]


class TestQueryMariadb:
    """Tests for query_mariadb tool."""

    async def test_valid_select(self) -> None:
        md = _make_mysql_backend(name="testmariadb", db_type="mariadb")
        ctx = _make_ctx({"testmariadb": md})
        result = await query_mariadb("SELECT * FROM users", ctx)
        assert "| id | name |" in result

    async def test_routes_to_mariadb_backend_only(self) -> None:
        """query_mariadb must NOT pick a mysql backend if no mariadb is configured.
        This is important because the timeout SQL differs between flavors."""
        mysql_backend = _make_mysql_backend(name="just_mysql", db_type="mysql")
        ctx = _make_ctx({"just_mysql": mysql_backend})
        result = await query_mariadb("SELECT 1", ctx)
        assert "Error:" in result
        assert "No mariadb database configured" in result

    async def test_analyze_select_rejected(self) -> None:
        """MariaDB's ANALYZE SELECT executes the inner query — must be rejected
        by the validator."""
        ctx = _make_ctx({"testmariadb": _make_mysql_backend(db_type="mariadb")})
        result = await query_mariadb("ANALYZE SELECT * FROM users", ctx)
        assert "Error:" in result

    async def test_mixed_mysql_and_mariadb(self) -> None:
        """With both configured, each tool should find its own backend."""
        mysql = _make_mysql_backend(name="m1", db_type="mysql")
        mariadb = _make_mysql_backend(name="m2", db_type="mariadb")
        ctx = _make_ctx({"m1": mysql, "m2": mariadb})

        await query_mysql("SELECT 1", ctx, database="m1")
        mysql.execute.assert_called_once()
        mariadb.execute.assert_not_called()

        await query_mariadb("SELECT 1", ctx, database="m2")
        mariadb.execute.assert_called_once()


class TestCrossTypeRouting:
    """Explicit `database=` must not cross flavor boundaries.

    Before the type-enforcement fix, a caller could pass a postgres connection
    name to query_mariadb and the tool would happily run MariaDB-flavored
    timeout SQL against a PostgreSQL backend. These tests lock in the fix:
    when db_type and name are both given and the named backend is a different
    type, the tool errors with a message that points at the correct tool
    instead of silently misrouting.
    """

    async def test_query_mariadb_rejects_mysql_connection_name(self) -> None:
        mysql = _make_mysql_backend(name="prod_mysql", db_type="mysql")
        mariadb = _make_mysql_backend(name="prod_mariadb", db_type="mariadb")
        ctx = _make_ctx({"prod_mysql": mysql, "prod_mariadb": mariadb})

        result = await query_mariadb("SELECT 1", ctx, database="prod_mysql")
        assert "Error:" in result
        assert "'prod_mysql' is a mysql connection, not mariadb" in result
        # Points the AI at the correct tool
        assert "query_mysql" in result
        # Neither backend should have been asked to execute anything
        mysql.execute.assert_not_called()
        mariadb.execute.assert_not_called()

    async def test_query_mysql_rejects_mariadb_connection_name(self) -> None:
        mysql = _make_mysql_backend(name="prod_mysql", db_type="mysql")
        mariadb = _make_mysql_backend(name="prod_mariadb", db_type="mariadb")
        ctx = _make_ctx({"prod_mysql": mysql, "prod_mariadb": mariadb})

        result = await query_mysql("SELECT 1", ctx, database="prod_mariadb")
        assert "Error:" in result
        assert "'prod_mariadb' is a mariadb connection, not mysql" in result
        assert "query_mariadb" in result

    async def test_query_postgres_rejects_clickhouse_connection_name(self) -> None:
        pg = _make_pg_backend(name="pg1")
        ch = _make_ch_backend(name="ch1")
        ctx = _make_ctx({"pg1": pg, "ch1": ch})

        result = await query_postgres("SELECT 1", ctx, database="ch1")
        assert "Error:" in result
        assert "'ch1' is a clickhouse connection, not postgres" in result
        assert "query_clickhouse" in result

    async def test_query_clickhouse_rejects_mysql_connection_name(self) -> None:
        ch = _make_ch_backend(name="ch1")
        mysql = _make_mysql_backend(name="mysql1", db_type="mysql")
        ctx = _make_ctx({"ch1": ch, "mysql1": mysql})

        result = await query_clickhouse("SELECT 1", ctx, database="mysql1")
        assert "Error:" in result
        assert "'mysql1' is a mysql connection, not clickhouse" in result

    async def test_shared_tools_still_accept_any_type_by_name(self) -> None:
        """list_tables / describe_table / sample_table / explain_query do
        NOT pass db_type to _get_backend — they dispatch on whatever the named
        connection happens to be. Verify the enforcement doesn't over-fire
        and break these shared tools."""
        mysql = _make_mysql_backend(name="any_backend", db_type="mysql")
        ctx = _make_ctx({"any_backend": mysql})

        # All of these should work against the mysql backend without error
        result = await list_tables("any_backend", ctx)
        assert "Error" not in result
        result = await describe_table("any_backend", "users", ctx)
        assert "Error" not in result


class TestMysqlSharedTools:
    """list_tables / describe_table / sample_table / explain_query should work
    against MySQL and MariaDB backends too, since those tools dispatch by name
    rather than hardcoding a type."""

    async def test_list_tables_mysql(self) -> None:
        ctx = _make_ctx({"testmysql": _make_mysql_backend()})
        result = await list_tables("testmysql", ctx)
        assert "users" in result
        assert "orders" in result

    async def test_describe_table_mysql(self) -> None:
        ctx = _make_ctx({"testmysql": _make_mysql_backend()})
        result = await describe_table("testmysql", "users", ctx)
        assert "| id | int unsigned | NO |" in result
        assert "| name | varchar(255) | YES |" in result

    async def test_describe_table_mysql_with_stats(self) -> None:
        my = _make_mysql_backend()
        my.table_stats = AsyncMock(
            return_value={
                "engine": "InnoDB",
                "table_rows_estimate": 12345,
                "primary_key": "id",
                "create_time": None,
                "update_time": None,
            }
        )
        ctx = _make_ctx({"testmysql": my})
        result = await describe_table("testmysql", "users", ctx)
        assert "**Table metadata:**" in result
        assert "engine: InnoDB" in result
        # The "_estimate" suffix is part of the contract — it tells the AI the
        # row count from information_schema is not reliable for InnoDB.
        assert "table_rows_estimate: 12345" in result

    async def test_sample_table_mysql(self) -> None:
        my = _make_mysql_backend()
        ctx = _make_ctx({"testmysql": my})
        result = await sample_table("testmysql", "users", ctx, n=3)
        my.execute.assert_called_once()
        sent_sql = my.execute.call_args[0][0]
        assert "SELECT * FROM users LIMIT 3" == sent_sql
        assert "| id | name |" in result

    async def test_explain_query_mysql(self) -> None:
        my = _make_mysql_backend()
        ctx = _make_ctx({"testmysql": my})
        result = await explain_query("SELECT * FROM users", "testmysql", ctx)
        assert "select_type: SIMPLE" in result
        my.explain.assert_called_once()

    async def test_explain_query_rejects_write_mysql(self) -> None:
        """EXPLAIN ANALYZE DELETE would execute the DELETE in MySQL 8.0.18+.
        The validator must reject the write before it reaches backend.explain."""
        ctx = _make_ctx({"testmysql": _make_mysql_backend()})
        result = await explain_query("DELETE FROM users", "testmysql", ctx)
        assert "Error:" in result

    async def test_list_databases_shows_flavor(self) -> None:
        """list_databases should label mysql and mariadb connections distinctly."""
        ctx = _make_ctx({
            "pg": _make_pg_backend(),
            "ch": _make_ch_backend(),
            "my": _make_mysql_backend(name="my", db_type="mysql"),
            "md": _make_mysql_backend(name="md", db_type="mariadb"),
        })
        result = await list_databases(ctx)
        assert "(mysql)" in result
        assert "(mariadb)" in result


class TestUsageGuideMysql:
    """usage_guide should mention MySQL and MariaDB tools."""

    async def test_guide_mentions_mysql_tools(self) -> None:
        ctx = _make_ctx({
            "my": _make_mysql_backend(name="my", db_type="mysql"),
            "md": _make_mysql_backend(name="md", db_type="mariadb"),
        })
        result = await usage_guide(ctx)
        assert "query_mysql" in result
        assert "query_mariadb" in result
        # Connections block should show both flavors
        assert "my" in result
        assert "md" in result


# ---------------------------------------------------------------------------
# SQLite tool path
# ---------------------------------------------------------------------------


class TestQuerySqlite:
    """Tests for query_sqlite tool."""

    async def test_valid_select(self) -> None:
        sq = _make_sqlite_backend()
        ctx = _make_ctx({"testsqlite": sq})
        result = await query_sqlite("SELECT * FROM users", ctx)
        assert "| id | name |" in result
        assert "| 1 | Alice |" in result
        sq.execute.assert_called_once_with("SELECT * FROM users")

    async def test_write_query_rejected(self) -> None:
        ctx = _make_ctx({"testsqlite": _make_sqlite_backend()})
        result = await query_sqlite("DELETE FROM users", ctx)
        assert "Error:" in result

    async def test_attach_database_rejected(self) -> None:
        """ATTACH DATABASE is SQLite-specific — make sure the validator
        catches it at the tool layer (the backend's VFS read-only mode is
        a second line of defense)."""
        ctx = _make_ctx({"testsqlite": _make_sqlite_backend()})
        result = await query_sqlite("ATTACH DATABASE '/tmp/other.db' AS o", ctx)
        assert "Error:" in result

    async def test_no_sqlite_configured(self) -> None:
        ctx = _make_ctx({"testpg": _make_pg_backend()})
        result = await query_sqlite("SELECT 1", ctx)
        assert "Error:" in result
        assert "No sqlite database configured" in result

    async def test_cross_type_rejection(self) -> None:
        """query_sqlite(database='a_postgres_conn') must fail fast with a
        pointed message — same policy as the other query_* tools."""
        pg = _make_pg_backend(name="pg1")
        sq = _make_sqlite_backend(name="sq1")
        ctx = _make_ctx({"pg1": pg, "sq1": sq})
        result = await query_sqlite("SELECT 1", ctx, database="pg1")
        assert "Error:" in result
        assert "'pg1' is a postgres connection, not sqlite" in result
        assert "query_postgres" in result

    async def test_json_output(self) -> None:
        import json as _json
        ctx = _make_ctx({"testsqlite": _make_sqlite_backend()})
        result = await query_sqlite("SELECT * FROM users", ctx, output_format="json")
        data = _json.loads(result)
        assert data["columns"] == ["id", "name"]
        assert data["rows"] == [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]


class TestSqliteSharedTools:
    """Shared tools (list_tables, describe_table, sample_table, explain_query)
    should dispatch to SQLite backends correctly."""

    async def test_list_tables_sqlite(self) -> None:
        ctx = _make_ctx({"testsqlite": _make_sqlite_backend()})
        result = await list_tables("testsqlite", ctx)
        assert "users" in result
        assert "orders" in result

    async def test_list_tables_sqlite_schema_param_surfaces_backend_error(self) -> None:
        """SQLite's backend raises ValueError when schema is given. The server
        tool forwards that as a clear error message."""
        sq = _make_sqlite_backend()
        sq.list_tables = AsyncMock(side_effect=ValueError(
            "SQLite does not support the `schema` parameter on list_tables."
        ))
        ctx = _make_ctx({"testsqlite": sq})
        result = await list_tables("testsqlite", ctx, schema="main")
        assert "Error:" in result
        assert "does not support" in result

    async def test_describe_table_sqlite(self) -> None:
        ctx = _make_ctx({"testsqlite": _make_sqlite_backend()})
        result = await describe_table("testsqlite", "users", ctx)
        assert "| id | INTEGER | NO |" in result
        assert "| name | TEXT | YES |" in result

    async def test_sample_table_sqlite(self) -> None:
        sq = _make_sqlite_backend()
        ctx = _make_ctx({"testsqlite": sq})
        result = await sample_table("testsqlite", "users", ctx, n=3)
        sq.execute.assert_called_once()
        sent_sql = sq.execute.call_args[0][0]
        assert "SELECT * FROM users LIMIT 3" == sent_sql
        assert "| id | name |" in result

    async def test_explain_query_sqlite(self) -> None:
        sq = _make_sqlite_backend()
        ctx = _make_ctx({"testsqlite": sq})
        result = await explain_query("SELECT * FROM users WHERE id = 1", "testsqlite", ctx)
        assert "SEARCH users" in result
        sq.explain.assert_called_once()

    async def test_list_databases_shows_sqlite(self) -> None:
        ctx = _make_ctx({"sq": _make_sqlite_backend(name="sq")})
        result = await list_databases(ctx)
        assert "(sqlite)" in result


class TestUsageGuideSqlite:
    async def test_guide_mentions_sqlite(self) -> None:
        ctx = _make_ctx({"sq": _make_sqlite_backend(name="sq")})
        result = await usage_guide(ctx)
        assert "query_sqlite" in result
        # SQLite-specific block should appear when SQLite is configured
        assert "SQLite" in result or "sqlite" in result
