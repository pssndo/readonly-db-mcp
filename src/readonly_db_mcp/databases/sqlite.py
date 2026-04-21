"""
SQLite backend using Python's stdlib sqlite3 module with read-only VFS enforcement.

SQLite is different from every other backend here because it's a file, not a
server. That means:
    - No host, port, user, password — just a filesystem path.
    - No network layer, no connection pool (cheap to open/close).
    - No per-user GRANT; read-only has to come from how the file is opened.
    - Python's stdlib `sqlite3` is synchronous; we wrap calls in
      `asyncio.to_thread()` the same way the ClickHouse backend does.

Layer 2 for SQLite: VFS-level read-only.
    We open the database via a URI: `file:/path/to/db?mode=ro` with
    `uri=True`. This tells SQLite itself to refuse any write at the storage
    layer — no journal creation, no B-tree mutation, no schema change.
    This is the strongest per-connection enforcement SQLite offers for the
    main database.

    We also explicitly disable extension loading (`enable_load_extension(False)`
    is the Python default, but we assert it). Without this, a malicious
    `SELECT load_extension('/tmp/evil.so')` would be a writable-side-effect
    bypass even on a read-only-opened DB. This is the SQLite equivalent of
    Postgres's "SELECT my_write_function()" concern.

    What VFS read-only does NOT block: `ATTACH DATABASE ... AS foo` succeeds
    even against a read-only main DB (SQLite attaches the secondary file,
    also in read-only mode by default, and reading from it works). Our only
    defense against ATTACH is Layer 1 (the sqlglot validator rejects
    `ATTACH` as `exp.Command`). If Layer 1 were ever relaxed, a caller could
    ATTACH another file by path and read it — still read-only, but breaking
    the "only the configured path is accessible" property that operators
    rely on. The validator-level rejection must stay.

Layer 3 for SQLite: operator file permissions.
    SQLite has no GRANT. The operator controls which .db file paths are
    reachable via `SQLITE_N_PATH` env vars. Treat the path as a privilege
    boundary the same way you treat a DB user's grants on PG/MySQL.

Security notes specific to SQLite:
    - ATTACH DATABASE: sqlglot parses `ATTACH` as `exp.Command` and the
      whitelist rejects it at the root. VFS read-only mode does NOT block
      ATTACH (attached DBs are also opened read-only, but the caller gets
      access to a path that wasn't in the server config — breaks the
      operator's "only this file is reachable" contract). Layer 1 is the
      only defense here.
    - load_extension: a function call, so looks like a pure SELECT to
      sqlglot. Blocked at Layer 2 via `enable_load_extension(False)`.
    - EXPLAIN QUERY PLAN is strictly plan-only (unlike PostgreSQL's
      `EXPLAIN ANALYZE` which executes). Safe to always run.
"""

import asyncio
import logging
import sqlite3
import time

from ..config import Config, SqliteConnection
from .base import DatabaseBackend, inject_limit, validate_identifier

logger = logging.getLogger("readonly_db_mcp.sqlite")

# Progress-handler tick: SQLite invokes the callback every N VDBE opcodes.
# 1000 is the SQLite docs' recommended "frequent enough to cancel long queries
# within a sensible latency, infrequent enough that overhead is negligible"
# default. On a modern CPU this fires roughly every few milliseconds of query
# work — good enough granularity for our timeout purpose.
_PROGRESS_HANDLER_OPCODES = 1000


class SqliteBackend(DatabaseBackend):
    """stdlib-sqlite3 backend with VFS-level read-only enforcement."""

    db_type = "sqlite"

    def __init__(self, conn_config: SqliteConnection, app_config: Config | None = None) -> None:
        self.path = conn_config.path
        self.name = conn_config.name
        # host/database fields are inherited from DatabaseBackend — fill them
        # with sensible values so list_databases output stays uniform. We use
        # the path for both: there's no distinction between "host" and
        # "database name" when the database is a single file.
        self.host = "(file)"
        self.database = conn_config.path
        self._conn: sqlite3.Connection | None = None
        self._timeout = app_config.query_timeout_seconds if app_config else 30
        self._max_rows = app_config.max_result_rows if app_config else 1000
        # Serialize access to the single connection. sqlite3.Connection is
        # thread-safe at the C layer but not reentrant in a way we can rely on
        # for our usage pattern: we share one connection across concurrent
        # asyncio.to_thread() calls (FastMCP can invoke tool methods in
        # parallel). Without this lock, two `cur.execute()` calls could hit
        # the same connection simultaneously and cause SQLITE_MISUSE or
        # surprising cursor state bleed-through. The lock is cheap (uncontended
        # for single-user workloads) and makes the invariant explicit.
        self._lock = asyncio.Lock()

    def _ensure_connected(self) -> sqlite3.Connection:
        """Return the connection, raising RuntimeError if not connected.

        We use RuntimeError instead of assert because Python's -O flag strips
        asserts, which would cause confusing AttributeError at runtime.
        """
        if self._conn is None:
            raise RuntimeError("Not connected — call connect() first")
        return self._conn

    def _open_readonly(self) -> sqlite3.Connection:
        """Open a connection to the SQLite database in read-only mode.

        Uses the URI form `file:...?mode=ro` with `uri=True`, which tells
        SQLite itself to refuse writes at the storage layer (VFS). Also
        explicitly disables extension loading — without this a malicious
        `SELECT load_extension('/tmp/evil.so')` could bypass read-only by
        running arbitrary code.

        check_same_thread=False is required because asyncio.to_thread()
        runs each call on an arbitrary pool thread, and sqlite3 otherwise
        rejects cross-thread use. The connection is only used serially
        (one to_thread call at a time per backend instance), so concurrency
        concerns don't apply. Callers that want concurrent queries should
        open multiple backends.
        """
        # Escape the path into a URI. We do NOT use urllib.parse.quote because
        # SQLite's URI parser expects a specific subset — and the operator
        # sets this path, not the AI, so path traversal isn't the concern.
        # We just guard against embedded '?' which would otherwise be
        # interpreted as a URI query separator.
        if "?" in self.path:
            raise ValueError(f"SQLite path contains '?' which conflicts with URI syntax: {self.path!r}")
        uri = f"file:{self.path}?mode=ro"
        conn = sqlite3.connect(
            uri,
            uri=True,
            # Note: sqlite3's `timeout` param is the busy-wait timeout for
            # acquiring a database lock — it is NOT a statement execution
            # timeout. Our per-query execution timeout is enforced via a
            # progress handler installed in `execute()` / `explain()`; see
            # _install_progress_handler below. We still pass a small lock
            # timeout so concurrent writers (if any, via a separate process)
            # don't cause us to fail immediately.
            timeout=min(self._timeout, 30),
            check_same_thread=False,  # serialized via self._lock — see __init__
        )
        # Defense-in-depth: explicitly disable extension loading even though
        # Python's default is already False. This matters because a library
        # the process imports could enable it globally — we want local
        # guarantees.
        #
        # Some Python/SQLite builds are compiled without
        # SQLITE_ENABLE_LOAD_EXTENSION (Debian/Ubuntu distro-stdlib, for
        # example). On those builds, `enable_load_extension` raises
        # NotSupportedError regardless of the argument — but the capability
        # we were trying to disable is already absent, so the defense is
        # achieved. Swallow the exception in that case and log at INFO so
        # operators on stricter builds can see the backend started fine.
        try:
            conn.enable_load_extension(False)
        except (sqlite3.NotSupportedError, AttributeError) as e:
            # AttributeError covers pysqlite builds that omit the method
            # entirely; NotSupportedError covers builds that stub it out.
            logger.info(
                "enable_load_extension not available for SQLite '%s' (%s) — "
                "extension loading is already disabled by build configuration, "
                "so the Layer 2 defense is still satisfied.",
                self.name, e,
            )
        return conn

    def _install_progress_handler(self, conn: sqlite3.Connection) -> None:
        """Install a progress handler that enforces `self._timeout` as an
        execution timeout (not just a lock-wait timeout).

        SQLite calls the handler every ~1000 VDBE opcodes (configurable).
        Returning a non-zero value aborts the current statement with
        `sqlite3.OperationalError: interrupted`. We record the start time
        in a closure and abort once wall-clock elapsed exceeds the configured
        timeout.

        The handler is cleared by `_clear_progress_handler()` after the
        statement completes (passing None as the callback). We install per-
        query rather than once-for-life because the deadline has to reset
        between queries.
        """
        deadline = time.monotonic() + self._timeout

        def _handler() -> int:
            # Non-zero return = abort. Returning 1 causes SQLite to raise
            # OperationalError("interrupted") from the .execute() call.
            if time.monotonic() > deadline:
                return 1
            return 0

        conn.set_progress_handler(_handler, _PROGRESS_HANDLER_OPCODES)

    def _clear_progress_handler(self, conn: sqlite3.Connection) -> None:
        """Remove the progress handler. Passing None disables it."""
        conn.set_progress_handler(None, 0)

    async def connect(self) -> None:
        """Open the read-only connection. Wrapped in to_thread because
        sqlite3.connect() is blocking (does file I/O)."""
        self._conn = await asyncio.to_thread(self._open_readonly)

    async def disconnect(self) -> None:
        """Close the connection.

        We take `self._lock` here for the same reason query methods do: if a
        query is in flight on a worker thread, closing the connection out from
        under it would crash with a sqlite3 misuse error. Waiting on the lock
        serializes shutdown behind any active query (bounded by the progress-
        handler timeout, so shutdown can't hang indefinitely).

        If the query is still queued waiting for the lock when disconnect
        fires, it will acquire the lock after us, find `self._conn is None`,
        and raise a clean RuntimeError("Not connected") from _ensure_connected.
        That's the intended "backend shut down" signal.
        """
        async with self._lock:
            if self._conn:
                await asyncio.to_thread(self._conn.close)
                self._conn = None

    async def execute(self, sql: str) -> tuple[list[str], list[tuple], int]:
        """Execute a read-only SELECT.

        No explicit transaction wrapping needed — the VFS-level read-only
        mode means the entire database is immutable for this connection's
        lifetime. No journal is created, no write can succeed. Layer 2 is
        enforced continuously, not per-query.

        sqlite3.Cursor.description is populated even for zero-row results,
        so column metadata survives empty queries (no prepare() dance like
        asyncpg needs).

        Execution timeout: a progress handler installed for the duration of
        this query aborts execution once `self._timeout` seconds elapse
        (see `_install_progress_handler`). Unlike sqlite3's `timeout`
        constructor param (lock-wait only), this actually bounds CPU/disk
        work done by a slow query.

        Concurrency: the `self._lock` guard ensures only one query uses the
        shared connection at a time, AND that disconnect() can't close the
        connection between our connection check and our use of it. Note that
        `_ensure_connected` is called INSIDE the lock — otherwise a shutdown
        running between the check and the lock acquisition could invalidate
        the connection we captured (TOCTOU).
        """
        # Inject LIMIT to cap row count. Parse with dialect="sqlite" so
        # SQLite-specific syntax (like |||-string-concat or `glob`) round-trips
        # correctly. This doesn't touch the connection so we can do it
        # outside the lock.
        fetch_limit = self._max_rows + 1
        limited_sql = inject_limit(sql, fetch_limit, dialect="sqlite")

        async with self._lock:
            conn = self._ensure_connected()  # inside the lock — see docstring

            def _run() -> tuple[list[str], list[tuple], int]:
                self._install_progress_handler(conn)
                cur = conn.cursor()
                try:
                    cur.execute(limited_sql)
                    columns = [d[0] for d in cur.description] if cur.description else []
                    rows = cur.fetchall()
                    return columns, rows, len(rows)
                finally:
                    cur.close()
                    self._clear_progress_handler(conn)

            columns, rows, _ = await asyncio.to_thread(_run)

        if not rows:
            return columns, [], 0
        total = len(rows)
        truncated = [tuple(r) for r in rows[: self._max_rows]]
        return columns, truncated, total

    async def list_tables(self, schema: str | None = None) -> list[str]:
        """List all user tables.

        SQLite has no concept of schemas in the usual sense (ATTACH DATABASE
        creates a schema-like namespace, but we don't allow ATTACH). So the
        `schema` param is intentionally not supported — if given, we raise a
        ValueError with a clear message instead of silently ignoring it,
        which would be worse.

        We exclude `sqlite_*` tables (SQLite's internal metadata — sqlite_master,
        sqlite_sequence, sqlite_stat1, etc.) since they're not usually what
        the AI wants to explore.
        """
        if schema is not None:
            raise ValueError(
                "SQLite does not support the `schema` parameter on list_tables. "
                "SQLite databases are single files with a single namespace."
            )

        async with self._lock:
            conn = self._ensure_connected()  # inside the lock — avoids TOCTOU with disconnect()

            def _run() -> list[str]:
                self._install_progress_handler(conn)
                cur = conn.cursor()
                try:
                    cur.execute(
                        "SELECT name FROM sqlite_master "
                        "WHERE type = 'table' AND name NOT LIKE 'sqlite_%' "
                        "ORDER BY name"
                    )
                    return [r[0] for r in cur.fetchall()]
                finally:
                    cur.close()
                    self._clear_progress_handler(conn)

            return await asyncio.to_thread(_run)

    async def describe_table(self, table: str) -> list[dict]:
        """Describe columns via `PRAGMA table_info(<table>)`.

        PRAGMA returns one row per column:
            (cid, name, type, notnull, dflt_value, pk)

        We map this to our standard `{name, type, nullable}` shape. SQLite
        uses "NOT NULL" flag semantics (1 = NOT NULL, 0 = nullable), which
        we invert to the "YES"/"NO" strings the server tool expects.

        Safety note: PRAGMA parameterization in sqlite3 is unreliable
        (different SQLite versions handle `?` binding inconsistently in
        PRAGMAs). We use f-string interpolation guarded by validate_identifier,
        which is the same defense-in-depth pattern the ClickHouse backend
        uses for DESCRIBE TABLE. validate_identifier rejects anything outside
        [A-Za-z_][A-Za-z0-9_.]*, so SQL injection via the table name is not
        possible.
        """
        safe_table = validate_identifier(table)

        async with self._lock:
            conn = self._ensure_connected()  # inside the lock — avoids TOCTOU with disconnect()

            def _run() -> list[dict]:
                self._install_progress_handler(conn)
                cur = conn.cursor()
                try:
                    # PRAGMA table_info doesn't support parameter binding
                    # reliably across versions; safe_table has already been
                    # validated.
                    cur.execute(f"PRAGMA table_info({safe_table})")
                    out = []
                    for row in cur.fetchall():
                        # row shape: (cid, name, type, notnull, dflt_value, pk)
                        _, name, col_type, notnull, _, _ = row
                        out.append({
                            "name": name,
                            "type": col_type or "",  # SQLite allows typeless columns
                            "nullable": "NO" if notnull else "YES",
                        })
                    return out
                finally:
                    cur.close()
                    self._clear_progress_handler(conn)

            return await asyncio.to_thread(_run)

    async def explain(self, sql: str, analyze: bool = False) -> str:
        """Return the query plan via `EXPLAIN QUERY PLAN`.

        SQLite's `EXPLAIN QUERY PLAN` is strictly plan-only — it never
        executes the inner query (unlike PostgreSQL's `EXPLAIN ANALYZE` or
        MySQL 8.0.18+'s `EXPLAIN ANALYZE`). So the `analyze` flag is
        accepted for interface consistency but has nothing useful to do;
        we append a note when it's requested.

        Safety: the `sql` parameter was already validated as read-only by
        the caller (`explain_query` tool → `validate_read_only`). EXPLAIN
        QUERY PLAN doesn't execute the query anyway, but we still rely on
        the validator's guarantee so this holds even if we switched to
        plain `EXPLAIN` (which outputs VDBE opcodes, also plan-only).
        """
        explain_sql = f"EXPLAIN QUERY PLAN {sql}"

        async with self._lock:
            conn = self._ensure_connected()  # inside the lock — avoids TOCTOU with disconnect()

            def _run() -> tuple[list[str], list[tuple]]:
                self._install_progress_handler(conn)
                cur = conn.cursor()
                try:
                    cur.execute(explain_sql)
                    columns = [d[0] for d in cur.description] if cur.description else []
                    rows = cur.fetchall()
                    return columns, rows
                finally:
                    cur.close()
                    self._clear_progress_handler(conn)

            columns, rows = await asyncio.to_thread(_run)

        # EXPLAIN QUERY PLAN returns (id, parent, notused, detail). The
        # `detail` column has the human-readable plan text; other columns
        # carry tree structure via the parent pointer. We render as indented
        # lines mirroring the tree depth, which is what the AI needs to read.
        lines: list[str] = []
        # Build a parent-to-children map so we can walk the tree depth-first.
        # Most SQLite plans are flat or shallow, so this is cheap.
        try:
            id_idx = columns.index("id")
            parent_idx = columns.index("parent")
            detail_idx = columns.index("detail")
        except ValueError:
            # Old SQLite or unexpected column order — fall back to raw rows.
            for row in rows:
                lines.append("  ".join(str(v) for v in row))
            plan = "\n".join(lines)
        else:
            children: dict[int, list[tuple]] = {}
            for r in rows:
                children.setdefault(r[parent_idx], []).append(r)

            def _walk(parent_id: int, depth: int) -> None:
                for r in children.get(parent_id, []):
                    lines.append("  " * depth + str(r[detail_idx]))
                    _walk(r[id_idx], depth + 1)

            _walk(0, 0)
            plan = "\n".join(lines) if lines else "(no plan rows)"

        if analyze:
            plan += (
                "\n\n*Note: SQLite's EXPLAIN is strictly plan-only (it never "
                "executes the inner query), so the `analyze` flag has no "
                "additional effect. Showing EXPLAIN QUERY PLAN output.*"
            )
        return plan
