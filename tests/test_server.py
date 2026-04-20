"""Tests for MCP server tool functions."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from readonly_db_mcp.config import Config
from readonly_db_mcp.server import (
    AppContext,
    _get_backend,
    query_postgres,
    query_clickhouse,
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
    """query_* tools should honor the output_format param."""

    async def test_json_format(self) -> None:
        import json as _json
        ctx = _make_ctx({"testpg": _make_pg_backend()})
        result = await query_postgres("SELECT * FROM users", ctx, output_format="json")
        data = _json.loads(result)
        assert data == [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

    async def test_vertical_format(self) -> None:
        ctx = _make_ctx({"testpg": _make_pg_backend()})
        result = await query_postgres("SELECT * FROM users", ctx, output_format="vertical")
        assert "-[ RECORD 1 ]-" in result

    async def test_invalid_format(self) -> None:
        ctx = _make_ctx({"testpg": _make_pg_backend()})
        result = await query_postgres("SELECT * FROM users", ctx, output_format="xml")
        assert "Error:" in result


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
