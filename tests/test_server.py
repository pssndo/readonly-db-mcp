"""Tests for MCP server tool functions."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from readonly_db_mcp.server import (
    AppContext,
    _get_backend,
    query_postgres,
    query_clickhouse,
    list_databases,
    list_tables,
    describe_table,
    explain_query,
)


def _make_ctx(backends: dict | None = None) -> MagicMock:
    """Create a mock MCP Context with the given backends."""
    app = AppContext(backends=backends or {})
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
