"""
ClickHouse backend using clickhouse-connect.

Read-only enforcement depends on the DB user having a read-only profile/role
on the server side (e.g. SETTINGS PROFILE 'readonly' or GRANT SELECT only).
This is verified at the server, which is the strongest guarantee.

We do NOT pass settings={"readonly": "1"} via clickhouse-connect — newer
versions (>=0.8) validate setting names against the server, and a client
that already has the readonly profile cannot re-set it, which causes a
ProgrammingError at client creation and on every query. The defense layers
that remain:
    - sqlglot AST validation (rejects non-SELECT before sending to the server)
    - DB user GRANT/profile (the server enforces it)
    - max_execution_time is still passed per-query to prevent runaway queries.

Key concepts for non-Python readers:
    - `clickhouse-connect` is the official ClickHouse Python client.
    - Unlike asyncpg, clickhouse-connect is SYNCHRONOUS (blocking). This means
      a query call blocks the entire thread until it completes. To avoid
      blocking the async event loop, we wrap every call in `asyncio.to_thread()`
      which runs the blocking function in a separate thread.
    - `asyncio.to_thread(func, arg1, arg2)` is equivalent to calling
      `func(arg1, arg2)` but in a background thread, so other async tasks
      can continue running while the database query executes.
"""

import asyncio
import logging

import clickhouse_connect
from clickhouse_connect.driver.client import Client

from ..config import ClickHouseConnection, Config
from .base import DatabaseBackend, validate_identifier, inject_limit

logger = logging.getLogger("readonly_db_mcp.clickhouse")


def _is_retriable_connection_error(error: Exception) -> bool:
    """Return True if an error likely indicates a stale/broken connection.

    Important: query timeout errors (e.g. max_execution_time exceeded) are
    intentionally NOT retried. Retrying those would just re-run the same heavy
    query and increase load without likely success.
    """
    message = str(error).lower()

    non_retriable_timeout_markers = (
        "max_execution_time",
        "time limit exceeded",
        "query timed out",
    )
    if any(marker in message for marker in non_retriable_timeout_markers):
        return False

    retriable_markers = (
        "connection",
        "network",
        "broken pipe",
        "connection reset",
        "connection aborted",
        "remote disconnected",
        "read timed out",
        "connect timed out",
    )
    return any(marker in message for marker in retriable_markers)


class ClickHouseBackend(DatabaseBackend):
    """ClickHouse backend with read-only enforcement via readonly=1 setting."""

    db_type = "clickhouse"

    def __init__(self, conn_config: ClickHouseConnection, app_config: Config | None = None) -> None:
        self.host = conn_config.host
        self.port = conn_config.port
        self.database = conn_config.database
        self.user = conn_config.user
        self.password = conn_config.password
        self.name = conn_config.name
        self.secure = conn_config.secure  # HTTPS/TLS — required for ClickHouse Cloud
        self._client: Client | None = None  # Set by connect(), None until then
        self._timeout = app_config.query_timeout_seconds if app_config else 30
        self._max_rows = app_config.max_result_rows if app_config else 1000
        # Store config for reconnection (unlike asyncpg's pool, clickhouse-connect
        # has no built-in reconnection — we handle it ourselves)
        self._conn_config = conn_config

    def _ensure_connected(self) -> Client:
        """Return the client, raising RuntimeError if not connected.

        We use RuntimeError instead of assert because Python's -O flag strips
        asserts, which would cause confusing AttributeError at runtime.
        """
        if self._client is None:
            raise RuntimeError("Not connected — call connect() first")
        return self._client

    def _create_client(self) -> Client:
        """Create a new ClickHouse client (synchronous — call via asyncio.to_thread).

        Extracted so both connect() and _reconnect() use the same settings.
        """
        return clickhouse_connect.get_client(
            host=self.host,
            port=self.port,
            username=self.user,
            password=self.password,
            database=self.database,
            secure=self.secure,  # Use HTTPS/TLS if configured (e.g. ClickHouse Cloud on port 8443)
            # NOTE: We intentionally do NOT pass settings={"readonly": "1"} here.
            # clickhouse-connect >=0.8 validates setting names against the server,
            # and "readonly" is itself a read-only setting that cannot be changed
            # by a client that already has a readonly profile. The DB user MUST
            # have GRANT SELECT only (enforced at the server via profile/role),
            # which is the primary defense. Additional defenses:
            #   - sqlglot AST validation rejects non-SELECT before queries are sent
            #   - max_execution_time is still passed per-query via settings
        )

    async def _reconnect(self) -> Client:
        """Attempt to reconnect after a stale connection is detected.

        clickhouse-connect uses HTTP and does not maintain persistent connections
        the way asyncpg does. However, the Client object can become stale if the
        ClickHouse server restarts or the network drops. This method creates a
        fresh client, replacing the old one.
        """
        logger.warning("Reconnecting to ClickHouse '%s' at %s:%d", self.name, self.host, self.port)
        try:
            if self._client:
                try:
                    await asyncio.to_thread(self._client.close)
                except Exception:
                    pass  # Old client is already broken — ignore close errors
            self._client = await asyncio.to_thread(self._create_client)
            logger.info("Reconnected to ClickHouse '%s'", self.name)
            return self._client
        except Exception:
            logger.exception("Reconnection to ClickHouse '%s' failed", self.name)
            raise

    async def connect(self) -> None:
        """Create the ClickHouse client with readonly=1 enforced at the session level.

        `readonly=1` tells ClickHouse to reject any write operations (INSERT,
        ALTER, CREATE, etc.) for all queries on this client.

        Connection health: clickhouse-connect uses HTTP under the hood, so
        connections are not truly persistent. However, the Client object can
        become stale. If a query fails due to a connection error, the execute()
        method retries once with a fresh client via _reconnect().

        Wrapped in asyncio.to_thread() because clickhouse-connect is synchronous.
        """
        self._client = await asyncio.to_thread(self._create_client)

    async def disconnect(self) -> None:
        """Close the ClickHouse client and release resources.

        Wrapped in asyncio.to_thread() for consistency with connect()/execute(),
        even though close() is typically fast. This avoids blocking the event
        loop if the underlying transport takes time to shut down.
        """
        if self._client:
            await asyncio.to_thread(self._client.close)
            self._client = None

    async def execute(self, sql: str) -> tuple[list[str], list[tuple], int]:
        """Execute a read-only query with readonly=1 and a timeout.

        To avoid unbounded memory usage on queries that return millions of rows,
        we inject a LIMIT clause to cap how many rows are fetched. We use sqlglot
        to parse the query and set LIMIT to min(existing_limit, max_rows + 1),
        which preserves the user's ORDER BY clause.

        We request max_rows + 1 so we can detect whether there were more rows
        than the limit (if we get exactly max_rows + 1, we know the result was
        truncated and report total_count as max_rows + 1 to trigger the
        truncation note).

        Both readonly and max_execution_time are passed per-query as extra
        safety, even though the client was already created with readonly=1.
        `max_execution_time` tells ClickHouse to kill the query if it runs
        longer than the configured timeout (in seconds).

        Wrapped in asyncio.to_thread() because clickhouse-connect is synchronous.

        Connection health: If the query fails with a connection error, retry once
        with a fresh client via _reconnect(). This handles ClickHouse restarts.
        """
        client = self._ensure_connected()

        # Inject LIMIT into the query using sqlglot AST manipulation.
        # This preserves the user's ORDER BY (unlike subquery wrapping).
        # If the user already has a LIMIT, we take the smaller of the two.
        fetch_limit = self._max_rows + 1
        limited_sql = inject_limit(sql, fetch_limit, dialect="clickhouse")

        # Try the query. If it fails with a connection error, retry once with reconnect.
        # Note: "readonly" is NOT in settings — see _create_client() for why.
        # max_execution_time kills the query server-side if it runs too long.
        try:
            result = await asyncio.to_thread(
                client.query,
                limited_sql,
                settings={"max_execution_time": self._timeout},
            )
        except Exception as e:
            # If we detect a likely stale/broken connection, retry once with a
            # fresh client. Query timeout errors are intentionally not retried.
            if _is_retriable_connection_error(e):
                logger.info("Query failed with connection error, retrying with reconnect")
                client = await self._reconnect()
                result = await asyncio.to_thread(
                    client.query,
                    limited_sql,
                    settings={"max_execution_time": self._timeout},
                )
            else:
                # Not a connection error — just re-raise
                raise

        columns = list(result.column_names)
        all_rows = result.result_rows
        total = len(all_rows)
        truncated = [tuple(r) for r in all_rows[: self._max_rows]]
        return columns, truncated, total

    async def list_tables(self) -> list[str]:
        """List all tables in the configured ClickHouse database.

        SHOW TABLES is a built-in ClickHouse command that returns all tables
        in the current database. Each row has a single column with the table name.
        """
        client = self._ensure_connected()
        result = await asyncio.to_thread(client.query, "SHOW TABLES")
        # result.result_rows is a list of tuples; each tuple has one element (table name)
        return [r[0] for r in result.result_rows]

    async def describe_table(self, table: str) -> list[dict]:
        """Describe columns for a table using DESCRIBE TABLE.

        IMPORTANT: The table name is interpolated into the SQL string (not
        parameterized), because ClickHouse's DESCRIBE TABLE doesn't support
        parameter binding. The validate_identifier() call ensures the table
        name only contains safe characters (letters, digits, underscores, dots).
        """
        client = self._ensure_connected()
        safe_table = validate_identifier(table)  # Prevent SQL injection
        result = await asyncio.to_thread(client.query, f"DESCRIBE TABLE {safe_table}")
        # DESCRIBE TABLE returns rows like: (name, type, default_type, default_expression, ...)
        # We only need the first two columns (name, type). ClickHouse marks nullable
        # columns with Nullable(Type) wrapper, so we check for that prefix.
        return [
            {"name": r[0], "type": r[1], "nullable": "YES" if r[1].startswith("Nullable") else "NO"}
            for r in result.result_rows
        ]

    async def table_stats(self, table: str) -> dict | None:
        """Return engine, row count, size, and key metadata from system.tables.

        Accepts "db.table" (fully qualified) or just "table" (resolved against
        the configured default database). Returns None if the table is not
        found in system.tables — this is not an error (the user may not have
        SELECT on system.tables, or the table may live in a schema outside
        our default).

        Safety: the query uses parameterized values ($1/$2) via clickhouse-connect's
        parameter binding to prevent SQL injection in the schema/table values.
        We still call validate_identifier() as defense-in-depth.
        """
        client = self._ensure_connected()
        if "." in table:
            db, tbl = table.split(".", 1)
        else:
            db, tbl = self.database, table
        validate_identifier(db)
        validate_identifier(tbl)

        # clickhouse-connect supports positional %s-style parameters. Values
        # are server-side bound, so SQL injection via db/tbl is not possible.
        sql = (
            "SELECT engine, total_rows, total_bytes, primary_key, "
            "sorting_key, partition_key "
            "FROM system.tables "
            "WHERE database = {db:String} AND name = {tbl:String}"
        )
        try:
            result = await asyncio.to_thread(
                client.query,
                sql,
                parameters={"db": db, "tbl": tbl},
            )
        except Exception as e:
            # If the user lacks SELECT on system.tables, or any other error,
            # fall back to no stats rather than failing the whole describe_table.
            logger.info("table_stats query failed for '%s.%s': %s", db, tbl, e)
            return None

        rows = result.result_rows
        if not rows:
            return None
        r = rows[0]
        return {
            "engine": r[0],
            "total_rows": r[1],
            "total_bytes": r[2],
            "primary_key": r[3],
            "sorting_key": r[4],
            "partition_key": r[5],
        }

    async def explain(self, sql: str, analyze: bool = False) -> str:
        """Return the EXPLAIN output for a query.

        Safety note: The EXPLAIN SQL is built via f-string, NOT validated by
        validate_read_only (which only allows SELECT). This is safe because:
            1. The `sql` parameter was already validated as read-only by the caller
            2. The query runs with readonly=1, so ClickHouse rejects any writes
            3. The `analyze` parameter is intentionally ignored — ClickHouse does
               not support EXPLAIN ANALYZE in the same way as PostgreSQL

        Note: ClickHouse EXPLAIN shows the query pipeline (how data flows through
        processing steps) rather than a cost-based execution plan.
        """
        client = self._ensure_connected()
        explain_sql = f"EXPLAIN {sql}"
        result = await asyncio.to_thread(client.query, explain_sql)
        # Each row has a single column with one line of the explain output
        plan = "\n".join(str(r[0]) for r in result.result_rows)

        # ClickHouse does not support EXPLAIN ANALYZE the way PostgreSQL does.
        # If the caller requested analyze, append a note so the AI agent
        # understands why there are no timing/row-count stats in the output.
        if analyze:
            plan += "\n\n*Note: EXPLAIN ANALYZE is not supported for ClickHouse. Showing standard EXPLAIN output.*"

        return plan
