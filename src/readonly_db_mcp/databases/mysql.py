"""
MySQL / MariaDB backend using asyncmy with read-only transaction enforcement.

This module exposes two concrete backends, `MySQLBackend` and `MariaDBBackend`,
that share all their implementation via a common base (`_MySQLFamilyBackend`).
They're distinct classes so each can declare its own class-level `db_type`
attribute — matching the pattern in postgres.py / clickhouse.py and letting
`_get_backend`'s type check work without per-instance shadowing.

Why share a base instead of duplicating? MySQL and MariaDB share the MySQL
wire protocol, asyncmy supports both, and sqlglot's "mysql" dialect handles
SELECT validation for both. The only runtime differences are:
    - `list_databases` label (driven by `db_type`)
    - The per-query timeout session variable
      (MySQL: `max_execution_time` in ms, SELECT-only;
       MariaDB: `max_statement_time` in seconds, all statements)
    - A couple of SQL dialect edges (e.g. MariaDB's `ANALYZE SELECT`, which
      we reject at the validator level anyway)
A shared base avoids copy-pasting the transaction/LIMIT/identifier logic.

Layer 2 (connection-level read-only) caveats for MySQL/MariaDB:
    Neither database has a per-connection "default_transaction_read_only"
    server setting the way PostgreSQL does. We approximate it by:
      - Running `SET SESSION TRANSACTION READ ONLY` as the pool's `init_command`
        (applies to all subsequent transactions on every pooled connection)
      - Wrapping each query in an explicit `START TRANSACTION READ ONLY` /
        `COMMIT` pair (per-query enforcement, defense-in-depth)
    This is slightly weaker than PG's pool-level flag because a malicious
    query could in theory issue `SET SESSION TRANSACTION READ WRITE` before
    its write — but Layer 1 (sqlglot) rejects `SET` statements at parse time,
    and Layer 3 (GRANT SELECT only) remains the authoritative backstop.

EXPLAIN ANALYZE safety: MySQL 8.0.18+ executes the inner query when EXPLAIN
ANALYZE is used (same semantics as PostgreSQL). Our `explain()` method
validates the inner SQL as read-only BEFORE prepending EXPLAIN, so
`EXPLAIN ANALYZE DELETE FROM users` can never reach the database.

Key concepts for non-Python readers:
    - `asyncmy` is a native-async MySQL driver (Cython, long2ice/asyncmy on
      GitHub). Unlike clickhouse-connect, it doesn't need asyncio.to_thread().
    - The pool API is aiomysql-compatible: `minsize`/`maxsize` (not `min_size`/
      `max_size` like asyncpg), and you close via `pool.close()` then
      `await pool.wait_closed()`.
    - `async with pool.acquire() as conn` borrows a connection from the pool.
      `async with conn.cursor() as cursor` opens a cursor (DB-API 2.0 style).
"""

import logging

import asyncmy

from ..config import Config, MariaDBConnection, MysqlConnection
from .base import DatabaseBackend, inject_limit, validate_identifier

logger = logging.getLogger("readonly_db_mcp.mysql")


class _MySQLFamilyBackend(DatabaseBackend):
    """Shared asyncmy implementation for MySQL and MariaDB.

    Subclasses declare their own class-level `db_type` ("mysql" or "mariadb").
    This base class is not registered as a backend directly — instantiate
    `MySQLBackend` or `MariaDBBackend` instead.
    """

    # Subclasses override. Declared here for type-checker clarity; the
    # _ensure_connected error message does not depend on it.
    db_type: str = "mysql"

    def __init__(
        self,
        conn_config: MysqlConnection | MariaDBConnection,
        app_config: Config | None = None,
    ) -> None:
        self.host = conn_config.host
        self.port = conn_config.port
        self.database = conn_config.database
        self.user = conn_config.user
        self.password = conn_config.password
        self.name = conn_config.name
        self._pool: asyncmy.Pool | None = None  # Set by connect(), None until then
        self._timeout = app_config.query_timeout_seconds if app_config else 30
        self._max_rows = app_config.max_result_rows if app_config else 1000

    def _ensure_connected(self) -> asyncmy.Pool:
        """Return the pool, raising RuntimeError if not connected.

        We use RuntimeError instead of assert because Python's -O flag strips
        asserts, which would cause confusing AttributeError at runtime.
        """
        if self._pool is None:
            raise RuntimeError("Not connected — call connect() first")
        return self._pool

    def _timeout_prelude_sql(self) -> str:
        """Return the session-variable SQL that sets the server-side timeout for
        the next statement. Runs inside the read-only transaction, before the
        user's query.

        - MySQL: `SET @@SESSION.max_execution_time = <ms>` — applies to SELECTs
          only (which is all we run). Unknown on MariaDB (would raise).
        - MariaDB: `SET @@SESSION.max_statement_time = <seconds>` — applies to
          all statements. Unknown on MySQL (would raise).

        We emit only the variant matching the subclass's db_type; the other
        would set an unknown variable and raise on the server.
        """
        if self.db_type == "mariadb":
            # MariaDB: max_statement_time is in seconds (float allowed)
            return f"SET @@SESSION.max_statement_time = {self._timeout}"
        # MySQL: max_execution_time is in milliseconds
        return f"SET @@SESSION.max_execution_time = {self._timeout * 1000}"

    async def connect(self) -> None:
        """Create a connection pool with session-level read-only enforced.

        asyncmy's `init_command` runs a single SQL statement after every new
        connection's handshake. We use it to set the session to read-only:
        `SET SESSION TRANSACTION READ ONLY` applies to every subsequent
        transaction on this connection, so even if a query somehow skipped the
        explicit per-query transaction wrapping (see `execute()`), the session
        would still refuse writes.

        Unlike asyncpg, asyncmy has no `server_settings=` dict — only a single
        `init_command` string. We therefore rely on per-query session SET
        statements for the timeout (emitted inside each query's transaction).
        """
        self._pool = await asyncmy.create_pool(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            db=self.database,
            minsize=1,  # Keep at least 1 connection ready
            maxsize=5,  # Allow up to 5 concurrent queries
            # init_command runs on every new connection right after handshake.
            # This makes the read-only setting sticky for the connection's life.
            init_command="SET SESSION TRANSACTION READ ONLY",
            autocommit=True,  # We manage transactions explicitly via START TRANSACTION
        )

    async def disconnect(self) -> None:
        """Close all connections in the pool and release resources.

        asyncmy's pool close pattern is two-step (close + wait_closed) — unlike
        asyncpg's single awaitable close. Follow the asyncmy docs exactly.
        """
        if self._pool:
            self._pool.close()
            await self._pool.wait_closed()
            self._pool = None

    async def execute(self, sql: str) -> tuple[list[str], list[tuple], int]:
        """Execute a read-only query inside an explicit read-only transaction.

        Belt-and-braces vs the session-level `SET SESSION TRANSACTION READ ONLY`
        from `init_command`: every query still runs `START TRANSACTION READ ONLY`
        explicitly. This matches the PG backend's `conn.transaction(readonly=True)`
        pattern and means both layers must fail for a write to succeed.

        The per-query timeout is set via a session variable on the same
        connection just before the user's query runs. See `_timeout_prelude_sql()`.
        """
        pool = self._ensure_connected()

        # Inject LIMIT to cap how many rows are fetched. We parse with
        # `dialect="mysql"` so MySQL-specific syntax (backtick quoting, etc.)
        # round-trips correctly; sqlglot's mysql dialect covers MariaDB too.
        fetch_limit = self._max_rows + 1
        limited_sql = inject_limit(sql, fetch_limit, dialect="mysql")

        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                # Set the server-side timeout for this session before the query.
                # This is per-connection state; pooled connections retain it
                # across queries, but setting it every time is cheap and avoids
                # surprises if the timeout was ever changed out-of-band.
                await cursor.execute(self._timeout_prelude_sql())
                # Explicit read-only transaction — Layer 2 defense-in-depth.
                await cursor.execute("START TRANSACTION READ ONLY")
                try:
                    await cursor.execute(limited_sql)
                    # cursor.description is populated even for zero-row results
                    # (DB-API 2.0 guarantee) — no prepare() dance needed.
                    columns = (
                        [desc[0] for desc in cursor.description] if cursor.description else []
                    )
                    rows = await cursor.fetchall()
                finally:
                    # Always close the transaction so the connection returns
                    # clean. COMMIT on a read-only txn is a no-op at the server.
                    await cursor.execute("COMMIT")

        if not rows:
            return columns, [], 0

        total = len(rows)
        truncated = [tuple(r) for r in rows[: self._max_rows]]
        return columns, truncated, total

    async def list_tables(self, schema: str | None = None) -> list[str]:
        """List user tables in a database.

        When `schema` is None, scope to the currently-connected database via
        `table_schema = DATABASE()`. When provided, scope to that schema instead
        (using a parameterized `%s` placeholder — no string interpolation).

        System schemas (mysql, information_schema, performance_schema, sys)
        are excluded defensively when no explicit schema is given. When the
        caller explicitly names one of those we honor it — if they want to
        look at information_schema, that's their choice (and still read-only).
        """
        pool = self._ensure_connected()
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(self._timeout_prelude_sql())
                await cursor.execute("START TRANSACTION READ ONLY")
                try:
                    if schema is None:
                        await cursor.execute(
                            """
                            SELECT table_name
                            FROM information_schema.tables
                            WHERE table_schema = DATABASE()
                              AND table_schema NOT IN ('mysql','information_schema','performance_schema','sys')
                            ORDER BY table_name
                            """
                        )
                    else:
                        await cursor.execute(
                            """
                            SELECT table_name
                            FROM information_schema.tables
                            WHERE table_schema = %s
                            ORDER BY table_name
                            """,
                            (schema,),
                        )
                    rows = await cursor.fetchall()
                finally:
                    await cursor.execute("COMMIT")
        return [r[0] for r in rows]

    async def describe_table(self, table: str) -> list[dict]:
        """Describe columns for a table in the currently-connected database.

        Uses a parameterized query (asyncmy placeholder is `%s`) to prevent
        SQL injection on the table name. The validate_identifier check is
        defense-in-depth.

        We return `column_type` (e.g. `varchar(255)`, `int unsigned`) rather
        than `data_type` (e.g. `varchar`, `int`) because the former includes
        length/precision/unsigned which AIs often need to understand the schema.
        """
        pool = self._ensure_connected()

        # Unlike PG where "public.users" is common, MySQL/MariaDB tables are
        # usually just "table_name" within the connected database. We accept
        # "db.table" too for cross-database references.
        if "." in table:
            schema, table_name = table.split(".", 1)
            validate_identifier(schema)
            validate_identifier(table_name)
        else:
            schema, table_name = None, table
            validate_identifier(table_name)

        pool = self._ensure_connected()
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(self._timeout_prelude_sql())
                await cursor.execute("START TRANSACTION READ ONLY")
                try:
                    if schema:
                        await cursor.execute(
                            """
                            SELECT column_name, column_type, is_nullable
                            FROM information_schema.columns
                            WHERE table_schema = %s AND table_name = %s
                            ORDER BY ordinal_position
                            """,
                            (schema, table_name),
                        )
                    else:
                        # Default to the currently-connected database
                        await cursor.execute(
                            """
                            SELECT column_name, column_type, is_nullable
                            FROM information_schema.columns
                            WHERE table_schema = DATABASE() AND table_name = %s
                            ORDER BY ordinal_position
                            """,
                            (table_name,),
                        )
                    rows = await cursor.fetchall()
                finally:
                    await cursor.execute("COMMIT")
        return [{"name": r[0], "type": r[1], "nullable": r[2]} for r in rows]

    async def table_stats(self, table: str) -> dict | None:
        """Return engine, row-count estimate, and primary key from information_schema.

        Important caveat: `information_schema.tables.table_rows` is an
        InnoDB-maintained ESTIMATE and can be wildly off (sometimes by 40-50%,
        sometimes reporting 0 for small tables). We surface this in the key
        name so AIs see the caveat. For an exact count they should issue
        `SELECT COUNT(*) FROM <table>`.

        Returns None if the table isn't found (permission issue or typo).
        """
        pool = self._ensure_connected()

        if "." in table:
            schema, table_name = table.split(".", 1)
            validate_identifier(schema)
            validate_identifier(table_name)
        else:
            schema, table_name = None, table
            validate_identifier(table_name)

        try:
            async with pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(self._timeout_prelude_sql())
                    await cursor.execute("START TRANSACTION READ ONLY")
                    try:
                        # Left-join to key_column_usage to assemble the PK as a
                        # comma-separated list of columns in ordinal order.
                        # Filtered on constraint_name='PRIMARY' (MySQL/MariaDB
                        # hardcode this name for primary key constraints).
                        if schema:
                            await cursor.execute(
                                """
                                SELECT t.engine, t.table_rows, t.create_time, t.update_time,
                                       GROUP_CONCAT(k.column_name ORDER BY k.ordinal_position) AS primary_key
                                FROM information_schema.tables t
                                LEFT JOIN information_schema.key_column_usage k
                                  ON k.table_schema = t.table_schema
                                 AND k.table_name = t.table_name
                                 AND k.constraint_name = 'PRIMARY'
                                WHERE t.table_schema = %s AND t.table_name = %s
                                GROUP BY t.engine, t.table_rows, t.create_time, t.update_time
                                """,
                                (schema, table_name),
                            )
                        else:
                            await cursor.execute(
                                """
                                SELECT t.engine, t.table_rows, t.create_time, t.update_time,
                                       GROUP_CONCAT(k.column_name ORDER BY k.ordinal_position) AS primary_key
                                FROM information_schema.tables t
                                LEFT JOIN information_schema.key_column_usage k
                                  ON k.table_schema = t.table_schema
                                 AND k.table_name = t.table_name
                                 AND k.constraint_name = 'PRIMARY'
                                WHERE t.table_schema = DATABASE() AND t.table_name = %s
                                GROUP BY t.engine, t.table_rows, t.create_time, t.update_time
                                """,
                                (table_name,),
                            )
                        row = await cursor.fetchone()
                    finally:
                        await cursor.execute("COMMIT")
        except Exception:
            # The common failure here is lack of SELECT on information_schema
            # (very rare — MySQL grants it by default for all users, filtered
            # to their own tables). We return None so that describe_table still
            # shows columns and the AI isn't blocked by a missing metadata row.
            #
            # But we also log the full exception at WARNING so real regressions
            # (connection drops, bad SQL after an information_schema change, a
            # timeout) are visible to operators tailing the server log. Without
            # this log, a silent None return would mask actual faults and make
            # debugging harder — which was the original reason this tool existed.
            logger.warning(
                "table_stats query failed for %r (returning None so describe_table "
                "continues without metadata)", table, exc_info=True,
            )
            return None

        if not row:
            return None
        engine, table_rows, create_time, update_time, primary_key = row
        return {
            "engine": engine,
            # Flag the estimate in the key name so the AI sees the caveat.
            "table_rows_estimate": table_rows,
            "create_time": create_time,
            "update_time": update_time,
            "primary_key": primary_key,
        }

    async def explain(self, sql: str, analyze: bool = False) -> str:
        """Return the EXPLAIN output for a query.

        Safety: the `sql` parameter was already validated as read-only by the
        caller (`explain_query` tool → `validate_read_only`). We do NOT expose
        `analyze=True` for MySQL because MySQL 8.0.18+ executes the inner
        query under EXPLAIN ANALYZE (same as PostgreSQL's EXPLAIN ANALYZE).
        Although the read-only transaction would catch writes, we prefer the
        simpler safety argument: plain EXPLAIN is strictly plan-only and
        cannot execute the query. The `analyze` flag is accepted but a note
        is appended instead of running EXPLAIN ANALYZE.

        MariaDB note: MariaDB's `ANALYZE <stmt>` also executes the query.
        Same conservative stance applies.
        """
        pool = self._ensure_connected()
        # We intentionally use plain EXPLAIN regardless of the analyze flag.
        # EXPLAIN without ANALYZE is strictly plan-only on both MySQL and MariaDB.
        explain_sql = f"EXPLAIN {sql}"
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(self._timeout_prelude_sql())
                await cursor.execute("START TRANSACTION READ ONLY")
                try:
                    await cursor.execute(explain_sql)
                    columns = (
                        [desc[0] for desc in cursor.description] if cursor.description else []
                    )
                    rows = await cursor.fetchall()
                finally:
                    await cursor.execute("COMMIT")

        # MySQL/MariaDB EXPLAIN output has multiple columns (id, select_type,
        # table, type, possible_keys, key, rows, Extra, ...). Render as
        # "col: value" per row so the AI can read the plan without losing
        # column context to an ASCII table.
        lines: list[str] = []
        for row in rows:
            for col, val in zip(columns, row):
                lines.append(f"  {col}: {val}")
            lines.append("")  # blank line between rows

        plan = "\n".join(lines).rstrip()
        if analyze:
            plan += (
                "\n\n*Note: EXPLAIN ANALYZE is not supported on "
                f"{self.db_type} via this tool because it executes the inner "
                "query. Showing plain EXPLAIN output (plan only, no execution).*"
            )
        return plan


class MySQLBackend(_MySQLFamilyBackend):
    """asyncmy-based MySQL backend with read-only enforcement.

    Uses `max_execution_time` (milliseconds, SELECT-only) for per-query
    timeouts. See `_MySQLFamilyBackend` for the shared implementation.
    """

    db_type = "mysql"


class MariaDBBackend(_MySQLFamilyBackend):
    """asyncmy-based MariaDB backend with read-only enforcement.

    Uses `max_statement_time` (seconds, applies to all statements) for
    per-query timeouts. See `_MySQLFamilyBackend` for the shared implementation.
    """

    db_type = "mariadb"
