"""
PostgreSQL backend using asyncpg with read-only transaction enforcement.

This is the second layer of write protection (after SQL validation):
    - The connection pool is created with `default_transaction_read_only = on`,
      which tells PostgreSQL to reject any write operation at the connection level.
    - Every query is additionally wrapped in an explicit read-only transaction
      via `conn.transaction(readonly=True)`.
    - Even if the SQL validator had a bug and let a write query through,
      PostgreSQL itself would reject it.

Key concepts for non-Python readers:
    - `asyncpg` is an async PostgreSQL driver. "Async" means it can handle
      multiple queries concurrently without blocking (important for a server).
    - A "connection pool" keeps several database connections open and reuses
      them, avoiding the overhead of connecting/disconnecting for every query.
    - `async with pool.acquire() as conn` borrows a connection from the pool
      and returns it when the block exits (even if an error occurs).
    - `async with conn.transaction(readonly=True)` starts a transaction that
      PostgreSQL will refuse to write in. If the block exits normally, the
      transaction commits (a no-op for reads). If an error occurs, it rolls back.
"""

from urllib.parse import quote

import asyncpg

from ..config import PostgresConnection, Config
from .base import DatabaseBackend, validate_identifier, inject_limit


class PostgresBackend(DatabaseBackend):
    """AsyncPG-based PostgreSQL backend with read-only enforcement."""

    db_type = "postgres"

    def __init__(self, conn_config: PostgresConnection, app_config: Config | None = None) -> None:
        self.host = conn_config.host
        self.port = conn_config.port
        self.database = conn_config.database
        self.user = conn_config.user
        self.password = conn_config.password
        self.name = conn_config.name
        self._pool: asyncpg.Pool | None = None  # Set by connect(), None until then
        self._timeout = app_config.query_timeout_seconds if app_config else 30
        self._max_rows = app_config.max_result_rows if app_config else 1000

    def _ensure_connected(self) -> asyncpg.Pool:
        """Return the pool, raising RuntimeError if not connected.

        We use RuntimeError instead of assert because Python's -O flag strips
        asserts, which would cause confusing AttributeError at runtime.
        """
        if self._pool is None:
            raise RuntimeError("Not connected — call connect() first")
        return self._pool

    async def connect(self) -> None:
        """Create a connection pool with read-only enforced at the connection level.

        The `server_settings` parameter tells PostgreSQL to set
        `default_transaction_read_only = on` for ALL connections in this pool.
        This means even if a write query somehow gets past our SQL validator,
        PostgreSQL itself will reject it with an error.

        `statement_timeout` is set as server-side enforcement: if a query
        runs longer than the configured timeout, PostgreSQL itself kills it.
        This complements asyncpg's client-side timeout — if the network
        connection hangs, asyncpg cancels; if the query is just slow,
        PostgreSQL cancels it server-side.

        Connection health: asyncpg's pool automatically validates connections
        when they are acquired. If PostgreSQL restarts, stale connections are
        detected and replaced transparently — no manual health checks needed.
        """
        # URL-encode user and password to handle special characters (@, :, /, %, #)
        # Without this, a password like "p@ss:word" would break the DSN URI parsing.
        # We use quote() (not quote_plus) because in the userinfo section of a URI,
        # spaces must be encoded as %20. quote_plus encodes spaces as "+", which is
        # only correct inside query strings — in userinfo "+" is treated as a literal.
        safe_user = quote(self.user, safe="")
        safe_password = quote(self.password, safe="")
        dsn = f"postgresql://{safe_user}:{safe_password}@{self.host}:{self.port}/{self.database}"
        self._pool = await asyncpg.create_pool(
            dsn=dsn,
            min_size=1,  # Keep at least 1 connection ready
            max_size=5,  # Allow up to 5 concurrent queries
            server_settings={
                "default_transaction_read_only": "on",  # Layer 2: PG-level read-only
                "statement_timeout": str(self._timeout * 1000),  # Server-side timeout (milliseconds)
            },
        )

    async def disconnect(self) -> None:
        """Close all connections in the pool and release resources."""
        if self._pool:
            await self._pool.close()
            self._pool = None

    async def execute(self, sql: str) -> tuple[list[str], list[tuple], int]:
        """Execute a read-only query within an explicit read-only transaction.

        To avoid unbounded memory usage on queries that return millions of rows,
        we inject a LIMIT clause to cap how many rows are fetched. We use sqlglot
        to parse the query and set LIMIT to min(existing_limit, max_rows + 1),
        which preserves the user's ORDER BY clause. (An earlier approach wrapped
        the user's SQL in a subquery, but PostgreSQL does not guarantee that
        ORDER BY inside a subquery propagates to the outer query.)

        We request max_rows + 1 so we can detect whether there were more rows
        than the limit (if we get exactly max_rows + 1, we know the result was
        truncated and report total_count as max_rows + 1 to trigger the
        truncation note).

        The `readonly=True` parameter is belt-and-suspenders with the pool-level
        `default_transaction_read_only`. Both are needed because:
            - Pool setting: covers all queries on this connection
            - Transaction setting: covers this specific query execution
        """
        pool = self._ensure_connected()

        # Inject LIMIT into the query using sqlglot AST manipulation.
        # This preserves the user's ORDER BY (unlike subquery wrapping).
        # If the user already has a LIMIT, we take the smaller of the two.
        fetch_limit = self._max_rows + 1
        limited_sql = inject_limit(sql, fetch_limit, dialect="postgres")

        async with pool.acquire() as conn:
            async with conn.transaction(readonly=True):
                # Use prepare + fetch so we can get column names even for
                # zero-row results. conn.fetch() returns an empty list for
                # zero-row queries, losing all column metadata.
                stmt = await conn.prepare(limited_sql)
                rows = await stmt.fetch(timeout=self._timeout)

        # Extract column names from the prepared statement's attributes,
        # which are available even when zero rows are returned.
        columns = [attr.name for attr in stmt.get_attributes()]

        if not rows:
            return columns, [], 0

        total = len(rows)
        truncated = [tuple(r.values()) for r in rows[: self._max_rows]]
        return columns, truncated, total

    async def list_tables(self) -> list[str]:
        """List all user tables in schema.table format, excluding PostgreSQL system schemas.

        pg_catalog and information_schema contain PostgreSQL's internal tables
        which are not useful for the AI to query.
        """
        pool = self._ensure_connected()
        async with pool.acquire() as conn:
            async with conn.transaction(readonly=True):
                rows = await conn.fetch(
                    """
                    SELECT schemaname || '.' || tablename AS table_name
                    FROM pg_tables
                    WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
                    ORDER BY schemaname, tablename
                    """,
                    timeout=self._timeout,
                )
        return [r["table_name"] for r in rows]

    async def describe_table(self, table: str) -> list[dict]:
        """Describe columns for a table. Accepts 'schema.table' or just 'table'.

        If no schema is specified, defaults to 'public' (PostgreSQL's default schema).
        Uses parameterized queries ($1, $2) to prevent SQL injection for the
        schema/table values. The validate_identifier check is defense-in-depth.
        """
        pool = self._ensure_connected()

        # Split "public.users" into schema="public", table_name="users"
        if "." in table:
            schema, table_name = table.split(".", 1)
        else:
            schema, table_name = "public", table

        # Validate both parts as safe identifiers (defense-in-depth — the $1/$2
        # parameterized query below already prevents SQL injection, but we
        # reject obviously malformed input early with a clear error message)
        validate_identifier(schema)
        validate_identifier(table_name)

        async with pool.acquire() as conn:
            async with conn.transaction(readonly=True):
                rows = await conn.fetch(
                    """
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_schema = $1 AND table_name = $2
                    ORDER BY ordinal_position
                    """,
                    schema,  # $1 — safe parameterized value
                    table_name,  # $2 — safe parameterized value
                    timeout=self._timeout,
                )
        return [{"name": r["column_name"], "type": r["data_type"], "nullable": r["is_nullable"]} for r in rows]

    async def explain(self, sql: str, analyze: bool = False) -> str:
        """Return the EXPLAIN output for a query.

        Safety note: The EXPLAIN SQL is built via f-string, NOT validated by
        validate_read_only (which only allows SELECT). This is safe because:
            1. The `sql` parameter was already validated as read-only by the caller
            2. The composed "EXPLAIN [ANALYZE] <sql>" runs inside a read-only
               transaction, so PostgreSQL itself prevents any side effects
            3. EXPLAIN ANALYZE in a read-only transaction executes the query
               for timing data but cannot commit any writes

        This safety depends on the read-only transaction — do NOT remove
        readonly=True from the transaction without also adding EXPLAIN
        validation at the SQL validator level.
        """
        pool = self._ensure_connected()
        explain_sql = f"EXPLAIN {'ANALYZE ' if analyze else ''}{sql}"
        async with pool.acquire() as conn:
            async with conn.transaction(readonly=True):
                rows = await conn.fetch(explain_sql, timeout=self._timeout)
        # Each row has a single "QUERY PLAN" column containing one line of the plan
        return "\n".join(r["QUERY PLAN"] for r in rows)
