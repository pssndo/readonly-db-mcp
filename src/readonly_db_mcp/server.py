"""
FastMCP server — tool definitions and lifespan management.

This is the entry point of the MCP server. It:
    1. Reads configuration (database connections) from environment variables
    2. Opens connections to all configured databases on startup
    3. Exposes tools that AI agents can call over the MCP protocol
    4. Cleans up all connections on shutdown

Key concepts for non-Python readers:
    - MCP (Model Context Protocol) is a standard for AI agents to call tools.
      This server speaks MCP over stdio (standard input/output), which is how
      Claude Code communicates with tool servers.
    - FastMCP is a Python framework that handles the MCP protocol details.
      We just define functions decorated with @mcp.tool() and FastMCP handles
      serialization, error handling, and protocol framing.
    - `@mcp.tool()` registers a function as a tool that AI agents can discover
      and call. The function's docstring becomes the tool's description, and
      its parameters become the tool's input schema.
    - The "lifespan" is an async context manager that runs setup code before
      the server starts accepting requests, and cleanup code after it stops.
    - `ctx: Context` is injected by FastMCP and gives each tool access to
      shared application state (the database backends).
"""

import logging
from contextlib import asynccontextmanager
from dataclasses import dataclass, field

from mcp.server.fastmcp import FastMCP, Context

from .config import load_config, Config
from .databases.base import DatabaseBackend, validate_identifier
from .databases.clickhouse import ClickHouseBackend
from .databases.postgres import PostgresBackend
from .formatting import format_results, format_markdown_table
from .validation import validate_read_only

# Module-level logger. Operators can configure the log level via standard
# Python logging config (e.g. LOGLEVEL env var or logging.basicConfig).
# By default, only WARNING and above are shown. Set to DEBUG for full
# connection and query tracing.
logger = logging.getLogger("readonly_db_mcp")


@dataclass
class AppContext:
    """Shared application state available to all tools via the lifespan context.

    This object is created once during startup and passed to every tool call
    through FastMCP's context system.
    """

    # Map of connection name -> backend instance (e.g. {"prod_db": PostgresBackend(...)})
    backends: dict[str, DatabaseBackend] = field(default_factory=dict)
    config: Config | None = None


@asynccontextmanager
async def lifespan(server: FastMCP):
    """Initialize database connections on startup, clean up on shutdown.

    This runs before the server starts accepting tool calls. It:
        1. Loads config from environment variables
        2. Creates and connects all database backends
        3. Yields the AppContext (making it available to tools)
        4. On shutdown (or error), disconnects all backends

    The try/finally ensures that if the 3rd database fails to connect,
    the first 2 that already connected are still properly closed.
    """
    config = load_config()
    ctx = AppContext(config=config)

    # Check for duplicate names across PG and CH connections. Backends are
    # stored in a flat dict keyed by name, so duplicates would silently overwrite.
    all_names = [pg.name for pg in config.postgres_connections] + [ch.name for ch in config.clickhouse_connections]
    seen: set[str] = set()
    for name in all_names:
        if name in seen:
            raise ValueError(f"Duplicate database connection name: {name!r}. Each connection must have a unique name.")
        seen.add(name)

    pg_count = len(config.postgres_connections)
    ch_count = len(config.clickhouse_connections)
    logger.info("Starting readonly-db-mcp with %d PostgreSQL and %d ClickHouse connections", pg_count, ch_count)

    try:
        # Initialize PostgreSQL connection pools
        for pg in config.postgres_connections:
            backend = PostgresBackend(pg, config)
            logger.info("Connecting to PostgreSQL '%s' at %s:%d/%s", pg.name, pg.host, pg.port, pg.database)
            try:
                await backend.connect()
                # Note: the backend is added to ctx.backends AFTER connect() succeeds.
                # If connect() raises, this backend is NOT in ctx.backends, so the
                # finally block won't try to disconnect it (no leak for the failing one).
                # All previously connected backends ARE in ctx.backends and WILL be
                # cleaned up by the finally block.
                ctx.backends[pg.name] = backend
                logger.info("Connected to PostgreSQL '%s'", pg.name)
            except Exception:
                # Graceful degradation: log the failure but continue with other backends.
                # The server can still be useful if at least one database connects.
                logger.exception("Failed to connect to PostgreSQL '%s' — skipping this backend", pg.name)

        # Initialize ClickHouse clients
        for ch in config.clickhouse_connections:
            backend = ClickHouseBackend(ch, config)
            logger.info("Connecting to ClickHouse '%s' at %s:%d/%s", ch.name, ch.host, ch.port, ch.database)
            try:
                await backend.connect()
                ctx.backends[ch.name] = backend
                logger.info("Connected to ClickHouse '%s'", ch.name)
            except Exception:
                logger.exception("Failed to connect to ClickHouse '%s' — skipping this backend", ch.name)

        # At least one backend must have succeeded, otherwise the server is useless
        if not ctx.backends:
            raise RuntimeError(
                f"Failed to connect to all {pg_count + ch_count} configured databases. "
                "Check connection settings and ensure at least one database is reachable."
            )

        logger.info("Successfully connected to %d of %d configured databases", len(ctx.backends), pg_count + ch_count)
        yield ctx  # Server is now running and accepting tool calls
    finally:
        # Clean up: disconnect all backends that were successfully connected.
        # Backends that failed to connect are not in ctx.backends (see note above).
        # Each disconnect is wrapped in try/except so one failing backend doesn't
        # prevent the others from being cleaned up (best-effort shutdown).
        logger.info("Shutting down — disconnecting %d backends", len(ctx.backends))
        for name, backend in ctx.backends.items():
            try:
                await backend.disconnect()
                logger.info("Disconnected '%s'", name)
            except Exception:
                logger.warning("Failed to disconnect '%s' (best-effort cleanup)", name, exc_info=True)


# ── Create the FastMCP server instance ───────────────────────────────────────
# "readonly-db-mcp" is the server name that AI agents see when they discover it.
# The lifespan function handles startup/shutdown of database connections.
mcp = FastMCP("readonly-db-mcp", lifespan=lifespan)


def _get_backend(ctx: Context, name: str | None, db_type: str | None = None) -> DatabaseBackend:
    """Resolve a database backend by name, or return the first matching type.

    Args:
        ctx:     The MCP context (contains the AppContext with all backends).
        name:    Specific connection name (e.g. "prod_db"). If provided, must
                 match exactly. If None, returns the first backend of db_type.
        db_type: Filter by type ("postgres" or "clickhouse"). Only used when
                 name is None.

    Raises:
        ValueError: If the named database doesn't exist, or no database of
                    the requested type is configured.
    """
    app: AppContext = ctx.request_context.lifespan_context
    if name:
        if name not in app.backends:
            raise ValueError(f"Unknown database: {name}. Available: {list(app.backends.keys())}")
        return app.backends[name]
    # No name given — return the first backend matching the requested type
    for backend in app.backends.values():
        if db_type is None or backend.db_type == db_type:
            return backend
    raise ValueError(f"No {db_type or 'any'} database configured")


def _safe_error_message(exc: Exception) -> str:
    """Extract a one-line error message from an exception, safe to show to the AI.

    Forwards the exception's message so the AI can see *why* the query failed
    (e.g. "Unknown table develop_db.foo" instead of a generic "execution failed").
    Stack traces never leak — we only take the first line of the message.

    Defense-in-depth: we cap the length so a pathological driver message (e.g.
    an embedded dump) can't flood the AI's context.
    """
    msg = str(exc).strip()
    if not msg:
        return exc.__class__.__name__
    # Take only the first line — drivers sometimes embed multi-line server
    # responses, which can be noisy (and occasionally leak internal details).
    first_line = msg.splitlines()[0]
    # Cap at a reasonable size
    if len(first_line) > 500:
        first_line = first_line[:497] + "..."
    return first_line


def _validate_output_format(fmt: str) -> str:
    """Validate the output_format param and return it, or raise ValueError."""
    if fmt not in ("table", "vertical", "json"):
        raise ValueError(
            f"Invalid output_format: {fmt!r}. Must be one of: 'table', 'vertical', 'json'."
        )
    return fmt


# ── Tool definitions ─────────────────────────────────────────────────────────
# Each @mcp.tool() function is a tool that the AI agent can call.
# All tools catch exceptions and return error strings (never stack traces)
# because the AI agent needs clear, actionable error messages.
#
# Discoverability: the docstrings below are what the AI sees when it inspects
# available tools. They cross-reference each other so the AI understands the
# full tool surface after reading any single one.


@mcp.tool()
async def query_postgres(
    sql: str,
    ctx: Context,
    database: str | None = None,
    output_format: str = "table",
) -> str:
    """Execute a read-only SELECT against PostgreSQL.

    Use this for ad-hoc queries with JOINs, aggregations, CTEs, etc. For common
    lookups there are dedicated tools that are faster and cheaper — prefer them:
      - `list_tables` / `list_databases` — discovery (don't write SHOW/DESC)
      - `describe_table` — columns + types (don't write DESCRIBE)
      - `sample_table` — peek at a few rows of a table
      - `explain_query` — execution plan (don't write EXPLAIN)

    Only SELECT-family queries pass validation. INSERT/UPDATE/DELETE/DDL are
    rejected by a sqlglot AST check before reaching the database.

    Params:
      sql:           The SQL query.
      database:      Connection name configured in env vars (e.g. "prod_db").
                     This is the *connection name*, not a PostgreSQL schema.
                     Defaults to the first configured PostgreSQL connection.
                     Use `list_databases` to see available connection names.
      output_format: "table" (markdown, default) | "vertical" (key=value per
                     row, no truncation — good for wide DDL/JSON cells) |
                     "json" (machine-readable, no truncation).

    Results are capped at MAX_RESULT_ROWS (default 1000) via a server-side
    LIMIT injection that preserves your ORDER BY.
    """
    try:
        fmt = _validate_output_format(output_format)
        # Layer 1: Validate the SQL is read-only using sqlglot AST analysis.
        # Use the returned clean SQL (stripped of trailing semicolons etc.)
        # because the raw input may break subquery wrapping in the backend.
        clean_sql = validate_read_only(sql, dialect="postgres")
        # Find the right PostgreSQL backend (by name, or the first PG backend)
        backend = _get_backend(ctx, database, db_type="postgres")
        logger.debug("query_postgres [%s]: %s", backend.name, clean_sql[:200])
        # Layer 2: Execute inside a read-only transaction (PG enforces this too)
        columns, rows, total = await backend.execute(clean_sql)
        logger.debug("query_postgres [%s]: %d columns, %d rows returned", backend.name, len(columns), len(rows))
        return format_results(columns, rows, total, output_format=fmt)
    except ValueError as e:
        logger.info("query_postgres validation error: %s", e)
        return f"Error: {e}"
    except Exception as e:
        # Never expose stack traces. Do forward the driver's error message so
        # the AI can see *why* the query failed ("Unknown table", "syntax error
        # at position N", etc.) — this information is already visible to anyone
        # who could send the query, so there's no new exposure.
        logger.exception("query_postgres execution failed")
        return f"Error: query execution failed: {_safe_error_message(e)}"


@mcp.tool()
async def query_clickhouse(
    sql: str,
    ctx: Context,
    database: str | None = None,
    output_format: str = "table",
) -> str:
    """Execute a read-only SELECT against ClickHouse.

    Use this for ad-hoc queries with JOINs, aggregations, window functions, etc.
    For common lookups there are dedicated tools that are faster and cheaper —
    prefer them:
      - `list_tables` / `list_databases` — discovery (don't write SHOW TABLES)
      - `describe_table` — columns + engine + row/byte stats + keys (don't
        write DESCRIBE)
      - `sample_table` — peek at a few rows of a table
      - `explain_query` — execution plan (don't write EXPLAIN)

    Only SELECT-family queries pass validation. INSERT/ALTER/CREATE/DROP are
    rejected by a sqlglot AST check before reaching the database.

    Params:
      sql:           The SQL query. Fully-qualify tables across schemas like
                     `develop_db.events` — the `database` param below picks
                     the *connection*, not the ClickHouse schema.
      database:      Connection name configured in env vars (e.g. "analytics").
                     This is the *connection name*, not a ClickHouse database.
                     Defaults to the first configured ClickHouse connection.
                     Use `list_databases` to see available connection names.
      output_format: "table" (markdown, default) | "vertical" (key=value per
                     row, no truncation — good for wide DDL/JSON cells) |
                     "json" (machine-readable, no truncation).

    Results are capped at MAX_RESULT_ROWS (default 1000) via a server-side
    LIMIT injection that preserves your ORDER BY.
    """
    try:
        fmt = _validate_output_format(output_format)
        clean_sql = validate_read_only(sql, dialect="clickhouse")
        backend = _get_backend(ctx, database, db_type="clickhouse")
        logger.debug("query_clickhouse [%s]: %s", backend.name, clean_sql[:200])
        columns, rows, total = await backend.execute(clean_sql)
        logger.debug("query_clickhouse [%s]: %d columns, %d rows returned", backend.name, len(columns), len(rows))
        return format_results(columns, rows, total, output_format=fmt)
    except ValueError as e:
        logger.info("query_clickhouse validation error: %s", e)
        return f"Error: {e}"
    except Exception as e:
        logger.exception("query_clickhouse execution failed")
        return f"Error: query execution failed: {_safe_error_message(e)}"


@mcp.tool()
async def list_databases(ctx: Context) -> str:
    """List all configured database connections.

    Each entry shows the *connection name* (what you pass to other tools as
    `database=...`), the backend type (postgres | clickhouse), and the
    host + default database. Start here when exploring — every other tool
    accepts a connection name from this list.

    Note: the connection name is not the same as a PostgreSQL schema or a
    ClickHouse database. It's an alias configured at server startup.
    """
    try:
        app: AppContext = ctx.request_context.lifespan_context
        lines = []
        for name, backend in app.backends.items():
            lines.append(f"- **{name}** ({backend.db_type}) — {backend.host}/{backend.database}")
        if not lines:
            return "No databases configured."
        return "\n".join(lines) + "\n\n*Pass the bolded name as `database=` to other tools.*"
    except Exception as e:
        logger.exception("list_databases failed")
        return f"Error: failed to list databases: {_safe_error_message(e)}"


@mcp.tool()
async def list_tables(database: str, ctx: Context) -> str:
    """List all tables visible to the configured user in a database connection.

    For PostgreSQL, results are in `schema.table` format (system schemas like
    pg_catalog and information_schema are excluded). For ClickHouse, results
    are table names in the connection's default database only — to see tables
    in other schemas, run `SELECT name FROM system.tables WHERE database = 'other'`.

    Params:
      database: Connection name from `list_databases` (not a schema name).

    Next step: call `describe_table(database, table)` for columns and metadata,
    or `sample_table(database, table, n=5)` to see a few rows.
    """
    try:
        backend = _get_backend(ctx, database)
        tables = await backend.list_tables()
        if not tables:
            return "No tables found."
        return "\n".join(f"- {t}" for t in tables)
    except ValueError as e:
        return f"Error: {e}"
    except Exception as e:
        logger.exception("list_tables failed for '%s'", database)
        return f"Error: failed to list tables: {_safe_error_message(e)}"


@mcp.tool()
async def describe_table(database: str, table: str, ctx: Context) -> str:
    """Show columns, types, and (for ClickHouse) engine + row/byte stats + keys.

    This is the right tool to inspect a single table. For PostgreSQL, pass
    `schema.table` (or just `table` for the public schema). For ClickHouse,
    pass the table name — fully qualify as `db.table` if it's not in the
    connection's default database.

    Params:
      database: Connection name from `list_databases`.
      table:    Table name. PG: "schema.table" or "table" (public schema default).
                CH: "table" or "db.table".

    ClickHouse output also includes engine, total_rows, total_bytes, primary_key,
    sorting_key, and partition_key when available (requires SELECT on system.tables;
    skipped silently if the user doesn't have that grant).
    """
    try:
        backend = _get_backend(ctx, database)
        columns = await backend.describe_table(table)
        table_str = format_markdown_table(
            ["column", "type", "nullable"],
            [(c["name"], c["type"], c["nullable"]) for c in columns],
            len(columns),
        )

        # Add backend-specific stats (currently ClickHouse only) — appended
        # as a separate section so the column table stays readable.
        stats = await backend.table_stats(table)
        if stats:
            stat_lines = ["", "**Table metadata:**"]
            for k, v in stats.items():
                if v in (None, ""):
                    continue
                stat_lines.append(f"- {k}: {v}")
            if len(stat_lines) > 2:
                table_str += "\n" + "\n".join(stat_lines)
        return table_str
    except ValueError as e:
        return f"Error: {e}"
    except Exception as e:
        logger.exception("describe_table failed for '%s.%s'", database, table)
        return f"Error: failed to describe table: {_safe_error_message(e)}"


@mcp.tool()
async def sample_table(
    database: str,
    table: str,
    ctx: Context,
    n: int = 5,
    output_format: str = "table",
) -> str:
    """Return the first N rows of a table — the "give me a peek" tool.

    Internally issues `SELECT * FROM <table> LIMIT <n>`, which goes through the
    same SELECT-only validator + LIMIT injection as `query_postgres` /
    `query_clickhouse`. Safe to use freely.

    Params:
      database:      Connection name from `list_databases`.
      table:         Table name. PG: "schema.table" or "table" (public). CH:
                     "table" or "db.table".
      n:             How many rows to return (1..MAX_RESULT_ROWS, default 5).
      output_format: "table" | "vertical" | "json". See `query_postgres` for
                     details.

    For wide tables (many columns or long values), "vertical" is often more
    readable than the default markdown table.
    """
    try:
        fmt = _validate_output_format(output_format)
        if n < 1:
            raise ValueError(f"n must be >= 1 (got {n})")

        backend = _get_backend(ctx, database)

        # Validate the table identifier (prevents SQL injection — we interpolate
        # into the SELECT since we can't parameterize table names). For PG, the
        # identifier may be "schema.table"; for CH it may be "db.table". Dots
        # are allowed by validate_identifier.
        safe_table = validate_identifier(table)

        # Cap n against the backend's MAX_RESULT_ROWS as well — the backend's
        # LIMIT injection would do this anyway, but catching it here gives a
        # clearer error if the user asks for something absurd.
        app: AppContext = ctx.request_context.lifespan_context
        max_rows = app.config.max_result_rows if app.config else 1000
        if n > max_rows:
            raise ValueError(f"n must be <= {max_rows} (MAX_RESULT_ROWS)")

        sql = f"SELECT * FROM {safe_table} LIMIT {int(n)}"

        # Full validation path — even though we built this SQL ourselves, we
        # run it through validate_read_only so the security contract holds
        # uniformly (no "trusted caller" exceptions).
        dialect = "postgres" if backend.db_type == "postgres" else "clickhouse"
        clean_sql = validate_read_only(sql, dialect=dialect)

        columns, rows, total = await backend.execute(clean_sql)
        return format_results(columns, rows, total, output_format=fmt)
    except ValueError as e:
        return f"Error: {e}"
    except Exception as e:
        logger.exception("sample_table failed for '%s.%s'", database, table)
        return f"Error: sample_table failed: {_safe_error_message(e)}"


@mcp.tool()
async def explain_query(sql: str, database: str, ctx: Context, analyze: bool = False) -> str:
    """Show the execution plan for a SELECT query.

    The inner query is validated as read-only BEFORE being wrapped in EXPLAIN,
    so EXPLAIN ANALYZE (which actually runs the query in PostgreSQL) can't
    execute a DELETE or INSERT.

    Params:
      database: Connection name from `list_databases`.
      sql:      The SELECT query to explain.
      analyze:  PostgreSQL only. If true, runs EXPLAIN ANALYZE (the query
                executes and real timing is shown, inside a read-only
                transaction). Ignored for ClickHouse (which has no equivalent).

    Use this before running expensive queries to check that indexes are hit
    and the row count estimate is reasonable.
    """
    try:
        backend = _get_backend(ctx, database)
        # Determine the SQL dialect from the backend type so the validator
        # parses the SQL correctly for the target database
        dialect = "postgres" if backend.db_type == "postgres" else "clickhouse"
        clean_sql = validate_read_only(sql, dialect=dialect)
        plan = await backend.explain(clean_sql, analyze=analyze)
        return plan
    except ValueError as e:
        return f"Error: {e}"
    except Exception as e:
        logger.exception("explain_query failed for '%s'", database)
        return f"Error: failed to explain query: {_safe_error_message(e)}"


@mcp.tool()
async def usage_guide(ctx: Context) -> str:
    """Return a cheatsheet for this MCP server — call once to learn the full surface.

    Use this when you're not sure which tool to reach for, or when raw SQL keeps
    getting rejected and you want to know which dedicated tool covers the use case.
    """
    app: AppContext = ctx.request_context.lifespan_context
    configured = "\n".join(
        f"  - `{name}` ({b.db_type}) → {b.host}/{b.database}"
        for name, b in app.backends.items()
    ) or "  (none)"

    return f"""# readonly-db-mcp — usage guide

## Configured connections
{configured}

Pass the connection name (left column) as `database=` to every tool below.
It is **not** a PostgreSQL schema or ClickHouse database — it's an alias
set up by the operator at server startup.

## Tools (prefer dedicated tools over raw SQL where possible)

| Tool              | When to use                                              |
| ----------------- | -------------------------------------------------------- |
| `list_databases`  | See configured connections. Start here when exploring.   |
| `list_tables`     | See tables in a connection. Don't write `SHOW TABLES`.   |
| `describe_table`  | Columns + types (CH also returns engine, row/byte stats, |
|                   | keys). Don't write `DESCRIBE`.                           |
| `sample_table`    | First N rows (default 5). Replaces "SELECT * LIMIT 5".   |
| `query_postgres`  | Ad-hoc SELECT against a PostgreSQL connection.           |
| `query_clickhouse`| Ad-hoc SELECT against a ClickHouse connection.           |
| `explain_query`   | Execution plan. Don't write `EXPLAIN`.                   |

## Typical workflow
1. `list_databases` → pick a connection
2. `list_tables(database=...)` → find a table
3. `describe_table(database=..., table=...)` → understand its shape
4. `sample_table(database=..., table=...)` → see a few rows
5. `query_*(...)` → run your real query

## `output_format` (on query_* and sample_table)
- `"table"` (default) — compact markdown table; cells >200 chars truncate.
  For single-row results with wide cells, auto-switches to vertical so you
  don't lose data.
- `"vertical"` — psql `\\gx`-style one-column-per-line. Best for wide values
  (DDL strings, JSON blobs), or single-row lookups.
- `"json"` — machine-readable JSON array of objects, no cell truncation.

## ClickHouse schemas
The `database` param picks the *connection*, not the schema. Fully qualify
tables that live outside the connection's default schema:
  `SELECT * FROM develop_db.events LIMIT 10`
To list tables in a different schema:
  `SELECT name FROM system.tables WHERE database = 'develop_db'`

## Write safety (informational)
All write operations (INSERT/UPDATE/DELETE/DDL) are rejected at the SQL
parse layer. The DB user should also have SELECT-only grants. Raw
`SHOW`/`DESCRIBE`/`EXPLAIN`/`EXISTS` commands are rejected too — use the
dedicated tools above instead.

## Result limits
Every query is capped at `MAX_RESULT_ROWS` (default 1000) via LIMIT
injection that preserves your `ORDER BY`. If the cap is hit, the output
includes a "*results truncated*" note.
"""


def main() -> None:
    """Entry point for the `readonly-db-mcp` CLI command.

    Runs the MCP server on stdio transport, which is how Claude Code
    communicates with MCP tool servers. The server reads JSON-RPC messages
    from stdin and writes responses to stdout.

    Logging goes to stderr (not stdout) to avoid corrupting the MCP protocol
    stream. Set the LOGLEVEL env var to control verbosity:
        LOGLEVEL=DEBUG   — full query and connection tracing
        LOGLEVEL=INFO    — connection lifecycle events
        LOGLEVEL=WARNING — only problems (default)
    """
    import os

    log_level = os.environ.get("LOGLEVEL", "WARNING").upper()
    logging.basicConfig(
        level=getattr(logging, log_level, logging.WARNING),
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        # stderr is critical: stdout is the MCP protocol transport
        stream=__import__("sys").stderr,
    )
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
