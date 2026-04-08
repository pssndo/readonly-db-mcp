"""
FastMCP server — tool definitions and lifespan management.

This is the entry point of the MCP server. It:
    1. Reads configuration (database connections) from environment variables
    2. Opens connections to all configured databases on startup
    3. Exposes six tools that AI agents can call over the MCP protocol
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
from .databases.base import DatabaseBackend
from .databases.clickhouse import ClickHouseBackend
from .databases.postgres import PostgresBackend
from .formatting import format_markdown_table
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


# ── Tool definitions ─────────────────────────────────────────────────────────
# Each @mcp.tool() function is a tool that the AI agent can call.
# All tools catch exceptions and return error strings (never stack traces)
# because the AI agent needs clear, actionable error messages.


@mcp.tool()
async def query_postgres(sql: str, ctx: Context, database: str | None = None) -> str:
    """Execute a read-only SQL query against PostgreSQL. Returns results as a markdown table."""
    try:
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
        # Format the results as a markdown table for the AI to read
        return format_markdown_table(columns, rows, total)
    except ValueError as e:
        logger.info("query_postgres validation error: %s", e)
        return f"Error: {e}"
    except Exception:
        # Never expose stack traces to the AI agent
        logger.exception("query_postgres execution failed")
        return "Error: query execution failed"


@mcp.tool()
async def query_clickhouse(sql: str, ctx: Context, database: str | None = None) -> str:
    """Execute a read-only SQL query against ClickHouse. Returns results as a markdown table."""
    try:
        clean_sql = validate_read_only(sql, dialect="clickhouse")
        backend = _get_backend(ctx, database, db_type="clickhouse")
        logger.debug("query_clickhouse [%s]: %s", backend.name, clean_sql[:200])
        columns, rows, total = await backend.execute(clean_sql)
        logger.debug("query_clickhouse [%s]: %d columns, %d rows returned", backend.name, len(columns), len(rows))
        return format_markdown_table(columns, rows, total)
    except ValueError as e:
        logger.info("query_clickhouse validation error: %s", e)
        return f"Error: {e}"
    except Exception:
        logger.exception("query_clickhouse execution failed")
        return "Error: query execution failed"


@mcp.tool()
async def list_databases(ctx: Context) -> str:
    """List all configured database connections."""
    try:
        app: AppContext = ctx.request_context.lifespan_context
        lines = []
        for name, backend in app.backends.items():
            lines.append(f"- **{name}** ({backend.db_type}) — {backend.host}/{backend.database}")
        return "\n".join(lines) if lines else "No databases configured."
    except Exception:
        logger.exception("list_databases failed")
        return "Error: failed to list databases"


@mcp.tool()
async def list_tables(database: str, ctx: Context) -> str:
    """List all tables in a configured database."""
    try:
        backend = _get_backend(ctx, database)
        tables = await backend.list_tables()
        return "\n".join(f"- {t}" for t in tables) if tables else "No tables found."
    except ValueError as e:
        return f"Error: {e}"
    except Exception:
        logger.exception("list_tables failed for '%s'", database)
        return "Error: failed to list tables"


@mcp.tool()
async def describe_table(database: str, table: str, ctx: Context) -> str:
    """Describe columns, types, and nullability for a table."""
    try:
        backend = _get_backend(ctx, database)
        columns = await backend.describe_table(table)
        # Reformat the column metadata into a markdown table
        return format_markdown_table(
            ["column", "type", "nullable"],
            [(c["name"], c["type"], c["nullable"]) for c in columns],
            len(columns),
        )
    except ValueError as e:
        return f"Error: {e}"
    except Exception:
        logger.exception("describe_table failed for '%s.%s'", database, table)
        return "Error: failed to describe table"


@mcp.tool()
async def explain_query(sql: str, database: str, ctx: Context, analyze: bool = False) -> str:
    """Show the execution plan for a query. Set analyze=True to run EXPLAIN ANALYZE (PostgreSQL only — ignored for ClickHouse)."""
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
    except Exception:
        logger.exception("explain_query failed for '%s'", database)
        return "Error: failed to explain query"


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
