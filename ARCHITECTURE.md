# readonly-db-mcp Architecture

This document describes the architecture, design decisions, and implementation details of readonly-db-mcp — a production-grade MCP server that provides safe, read-only SQL access to PostgreSQL, ClickHouse, MySQL, MariaDB, and SQLite databases for AI agents.

**For user-facing documentation**, see [README.md](README.md).

---

## Overview

readonly-db-mcp is a pip-installable MCP server that gives AI agents (Claude Code, Cursor, etc.) safe, **read-only** SQL access to PostgreSQL, ClickHouse, MySQL, MariaDB, and SQLite databases. Three independent layers of write protection ensure that even a compromised or malicious AI agent cannot modify production data.

**Design priorities:**
- **Security first**: Three-layer defense-in-depth architecture
- **Zero trust**: Every query is validated, every connection is read-only, every user has minimal privileges
- **Operational simplicity**: `pip install`, one JSON config block, done
- **Production ready**: Connection pooling, health checks, structured logging, graceful degradation

---

## Architecture

```
Claude Code / AI Agent
        |
        | MCP protocol (stdio)
        v
+---------------------------+
|   readonly-db-mcp server  |
|                           |
|  1. sqlglot validation    |  <-- parse SQL AST, reject non-SELECT
|  2. connection pool       |  <-- asyncpg (PG) / clickhouse-connect (CH)
|                           |      asyncmy (MySQL + MariaDB)
|                           |      stdlib sqlite3 (SQLite, file-based)
|  3. read-only transaction |  <-- belt-and-suspenders enforcement
|  4. result formatting     |  <-- markdown tables for the AI
+---------------------------+
        |
        | SQL (read-only user, or VFS-level read-only for SQLite)
        v
  PostgreSQL    ClickHouse    MySQL    MariaDB    SQLite
```

### Three Layers of Write Protection

1. **sqlglot AST validation** -- whitelist approach: root node must be SELECT-family, full tree walk rejects any write node hidden in CTEs/subqueries
2. **Connection-level settings** -- `default_transaction_read_only=on` (PG), `readonly=1` (CH), `SET SESSION TRANSACTION READ ONLY` + per-query `START TRANSACTION READ ONLY` (MySQL/MariaDB), `file:?mode=ro` URI + `enable_load_extension(False)` (SQLite)
3. **DB user permissions** -- dedicated users with only `GRANT SELECT` (PG/CH/MySQL/MariaDB); for SQLite, the operator controls which `.db` file paths are reachable via `SQLITE_N_PATH` env vars

**MySQL/MariaDB caveat for Layer 2:** unlike PostgreSQL, MySQL and MariaDB have no server-side per-connection "default transaction read-only" flag. We approximate it by running `SET SESSION TRANSACTION READ ONLY` at connection init (via asyncmy's `init_command`) and wrapping every query in `START TRANSACTION READ ONLY` / `COMMIT`. This is slightly weaker than the PG mechanism because a hypothetical query that bypassed our transaction wrapping could toggle the session back to READ WRITE — but Layer 1 (sqlglot) rejects `SET` statements at parse time, so this gap only matters if Layer 1 were already compromised. **Layer 3 (GRANT SELECT only) is the authoritative backstop for MySQL/MariaDB** and must not be skipped.

**SQLite caveat for Layer 2:** the VFS-level read-only mode (`file:...?mode=ro`) is continuous and strong — no writes to the main database file can succeed, ever. But `ATTACH DATABASE` is *not* blocked by VFS read-only (SQLite will attach another file, also read-only by default, and allow reads from it). Layer 1 (the sqlglot validator) is the only defense against `ATTACH` — it parses as `exp.Command` and is rejected at the whitelist root check. Similarly, `SELECT load_extension(...)` parses as a pure function call and passes Layer 1, so Layer 2 must separately disable extension loading via `conn.enable_load_extension(False)` at connect time. Both are documented in the SQLite backend module with explicit notes so future changes don't accidentally weaken either defense.

---

## Tech Stack

| Component             | Package                     | Version | Why                                                           |
| --------------------- | --------------------------- | ------- | ------------------------------------------------------------- |
| MCP framework         | `mcp`                       | >=1.2.0 | Official Python SDK with FastMCP                              |
| SQL parsing           | `sqlglot`                   | >=26.0  | Supports `postgres`, `clickhouse`, `mysql`, `sqlite` dialects (MariaDB covered by mysql) |
| PostgreSQL driver     | `asyncpg`                   | >=0.29  | Fast async driver, native read-only transaction support       |
| ClickHouse driver     | `clickhouse-connect`        | >=0.7   | Official ClickHouse Python client                             |
| MySQL/MariaDB driver  | `asyncmy`                   | >=0.2.9 | Native-async Cython driver (aiomysql-compatible API). One driver serves both. |
| SQLite driver         | `sqlite3` (stdlib)          | n/a     | Python standard library. No new dep. Wrapped in `asyncio.to_thread()`. |
| Config                | `python-dotenv`             | >=1.0   | Env var / .env parsing (simple, no validation overhead)       |
| Testing               | `pytest` + `pytest-asyncio` | latest  | Async test support                                            |

Python >=3.11 required.

---

## Project Structure

```
readonly-db-mcp/
  ARCHITECTURE.md             # This file — architecture and design docs
  README.md                   # User-facing install + config docs
  CLAUDE.md                   # Project instructions for Claude Code
  pyproject.toml              # Package config, deps, entry point
  src/
    readonly_db_mcp/
      __init__.py             # Package version
      server.py               # FastMCP server, tool definitions, lifespan, logging
      config.py               # Env var parsing with python-dotenv
      validation.py           # sqlglot AST-based read-only query validator
      formatting.py           # Query results → markdown table conversion
      databases/
        __init__.py
        base.py               # Abstract DatabaseBackend + shared utilities
        postgres.py           # asyncpg pool, read-only transactions, health checks
        clickhouse.py         # clickhouse-connect client, readonly=1, reconnection
        mysql.py              # asyncmy pool, SESSION READ ONLY + per-query START TRANSACTION READ ONLY; serves both MySQL and MariaDB
        sqlite.py             # stdlib sqlite3 wrapped in asyncio.to_thread; VFS-level read-only open (file:?mode=ro); extension loading disabled
  tests/
    test_validation.py        # SQL validation tests (security-critical)
    test_config.py            # Config parsing tests
    test_formatting.py        # Result formatting tests
    test_backends.py          # Backend unit tests (validation, LIMIT injection)
    test_server.py            # MCP tool-level integration tests
```

---

## Configuration

Environment variables (or `.env` file). Multiple databases via numbered prefix.

```env
# PostgreSQL connections (PG_1_, PG_2_, etc.)
PG_1_NAME=prod_db
PG_1_HOST=pg-prod.internal
PG_1_PORT=5432
PG_1_DATABASE=myapp
PG_1_USER=ai_reader
PG_1_PASSWORD=secret

# ClickHouse connections (CH_1_, CH_2_, etc.)
CH_1_NAME=analytics
CH_1_HOST=ch-prod.internal
CH_1_PORT=8123
CH_1_DATABASE=events
CH_1_USER=ai_reader
CH_1_PASSWORD=secret

# MySQL connections (MYSQL_1_, MYSQL_2_, etc.)
MYSQL_1_NAME=primary_mysql
MYSQL_1_HOST=mysql-prod.internal
MYSQL_1_PORT=3306
MYSQL_1_DATABASE=myapp
MYSQL_1_USER=ai_reader
MYSQL_1_PASSWORD=secret

# MariaDB connections (MARIADB_1_, MARIADB_2_, etc.)
# Separate prefix from MySQL — same wire protocol, but distinct at the
# config layer so operators can be explicit about what they're targeting.
MARIADB_1_NAME=legacy
MARIADB_1_HOST=mariadb-prod.internal
MARIADB_1_PORT=3306
MARIADB_1_DATABASE=legacy_app
MARIADB_1_USER=ai_reader
MARIADB_1_PASSWORD=secret

# SQLite connections (SQLITE_1_, SQLITE_2_, etc.)
# SQLite is a single-file database — no host, port, user, or password.
# Operators control which file paths are reachable. The connection is
# opened in VFS-level read-only mode (`file:<path>?mode=ro`).
SQLITE_1_NAME=local_dev
SQLITE_1_PATH=/var/data/app.db

# Global settings
QUERY_TIMEOUT_SECONDS=30       # Per-query timeout
MAX_RESULT_ROWS=1000           # Truncate results beyond this
```

---

## Claude Code Setup (end user experience)

```bash
pip install readonly-db-mcp
```

Then add to `.claude/mcp.json` in the project:

```json
{
  "mcpServers": {
    "readonly-db": {
      "command": "readonly-db-mcp",
      "env": {
        "PG_1_NAME": "prod_db",
        "PG_1_HOST": "pg-prod.internal",
        "PG_1_PORT": "5432",
        "PG_1_DATABASE": "myapp",
        "PG_1_USER": "ai_reader",
        "PG_1_PASSWORD": "secret",
        "CH_1_NAME": "analytics",
        "CH_1_HOST": "ch-prod.internal",
        "CH_1_PORT": "8123",
        "CH_1_DATABASE": "events",
        "CH_1_USER": "ai_reader",
        "CH_1_PASSWORD": "secret"
      }
    }
  }
}
```

Claude Code auto-discovers the tools. No further setup.

---

## MCP Tools

Eleven tools are exposed to the AI. Dedicated tools should be preferred over raw SQL where available — they're cheaper, clearer, and don't hit the "SELECT-only" rejection path.

### 1. `query_postgres`

Execute a read-only SQL query against PostgreSQL.

| Parameter       | Type   | Required | Description                                                     |
| --------------- | ------ | -------- | --------------------------------------------------------------- |
| `sql`           | string | yes      | The SQL query                                                   |
| `database`      | string | no       | Named PG **connection** (default: first configured)             |
| `output_format` | string | no       | `"table"` (default) \| `"vertical"` \| `"json"`                 |

Returns: results rendered in the requested format.

### 2. `query_clickhouse`

Execute a read-only SQL query against ClickHouse.

| Parameter       | Type   | Required | Description                                                     |
| --------------- | ------ | -------- | --------------------------------------------------------------- |
| `sql`           | string | yes      | The SQL query (fully-qualify tables outside the default schema) |
| `database`      | string | no       | Named CH **connection** (default: first configured)             |
| `output_format` | string | no       | `"table"` (default) \| `"vertical"` \| `"json"`                 |

Returns: results rendered in the requested format.

### 3. `query_mysql`

Execute a read-only SQL query against MySQL.

| Parameter       | Type   | Required | Description                                                     |
| --------------- | ------ | -------- | --------------------------------------------------------------- |
| `sql`           | string | yes      | The SQL query                                                   |
| `database`      | string | no       | Named MySQL **connection** (default: first configured)          |
| `output_format` | string | no       | `"table"` (default) \| `"vertical"` \| `"json"`                 |

Returns: results rendered in the requested format. Safety note: MySQL 8.0.18+ `EXPLAIN ANALYZE` executes the inner query, same as PostgreSQL — `explain_query` validates the inner SQL as read-only first, but raw `EXPLAIN ANALYZE ...` sent via `query_mysql` is rejected by the validator.

### 4. `query_mariadb`

Execute a read-only SQL query against MariaDB.

| Parameter       | Type   | Required | Description                                                     |
| --------------- | ------ | -------- | --------------------------------------------------------------- |
| `sql`           | string | yes      | The SQL query                                                   |
| `database`      | string | no       | Named MariaDB **connection** (default: first configured)        |
| `output_format` | string | no       | `"table"` (default) \| `"vertical"` \| `"json"`                 |

Returns: results rendered in the requested format. Separate tool from `query_mysql` because MariaDB has distinct timeout semantics (`max_statement_time` in seconds, vs MySQL's `max_execution_time` in ms) and operator-facing config (`MARIADB_N_*` prefix). Shares the MySQL wire protocol and sqlglot parser.

### 5. `query_sqlite`

Execute a read-only SQL query against a SQLite database file.

| Parameter       | Type   | Required | Description                                                     |
| --------------- | ------ | -------- | --------------------------------------------------------------- |
| `sql`           | string | yes      | The SQL query                                                   |
| `database`      | string | no       | Named SQLite **connection** (default: first configured)         |
| `output_format` | string | no       | `"table"` (default) \| `"vertical"` \| `"json"`                 |

Returns: results rendered in the requested format. SQLite is opened in VFS-level read-only mode at connect time (`file:<path>?mode=ro`), which means the database file cannot be modified for the life of the connection. `ATTACH DATABASE`, `PRAGMA writable_schema=ON`, and all DDL/DML are rejected at the sqlglot validation layer. `SELECT load_extension(...)` is blocked at connect time via `enable_load_extension(False)`. No pool is needed — SQLite is file-based and cheap to open.

### 6. `list_databases`

List all configured database connections.

| Parameter | Type | Required | Description |
| --------- | ---- | -------- | ----------- |
| (none)    |      |          |             |

Returns: list of `{name, type, host, database}`. The `name` is what you pass as `database=...` to other tools — it's an alias, **not** a PG schema or CH database.

### 7. `list_tables`

List tables in a database.

| Parameter  | Type   | Required | Description                                                  |
| ---------- | ------ | -------- | ------------------------------------------------------------ |
| `database` | string | yes      | Connection name                                              |
| `schema`   | string | no       | Restrict to a specific schema/database within the connection |

Returns: list of table names. When `schema` is omitted, each backend uses its natural default (all non-system schemas for PG; the connection's configured database for CH/MySQL/MariaDB). When `schema` is provided, the backend scopes its metadata query to that schema via a parameterized SELECT against `pg_tables` / `system.tables` / `information_schema.tables` — no string interpolation. PostgreSQL still returns `schema.table` format regardless, so the output shape is uniform.

### 8. `describe_table`

Show columns, types, and nullability for a table. For ClickHouse, also returns engine, total_rows, total_bytes, primary_key, sorting_key, and partition_key (read from `system.tables`, silently skipped if the user lacks that grant).

| Parameter  | Type   | Required | Description                                                    |
| ---------- | ------ | -------- | -------------------------------------------------------------- |
| `database` | string | yes      | Connection name                                                |
| `table`    | string | yes      | `schema.table` (PG) or `table` / `db.table` (CH)               |

Returns: markdown table of column definitions, plus an optional metadata section.

### 9. `sample_table`

Return the first N rows of a table — the "give me a peek" shortcut.

| Parameter       | Type   | Required | Description                                                  |
| --------------- | ------ | -------- | ------------------------------------------------------------ |
| `database`      | string | yes      | Connection name                                              |
| `table`         | string | yes      | Table name (same rules as `describe_table`)                  |
| `n`             | int    | no       | Number of rows (1..MAX_RESULT_ROWS, default 5)               |
| `output_format` | string | no       | `"table"` (default) \| `"vertical"` \| `"json"`              |

Internally issues `SELECT * FROM <safe_table> LIMIT <n>` through the same validation path as `query_*`. Table names are validated against the safe-identifier regex before interpolation.

### 10. `explain_query`

Show the query execution plan.

| Parameter  | Type    | Required | Description                                                  |
| ---------- | ------- | -------- | ------------------------------------------------------------ |
| `sql`      | string  | yes      | The SELECT query to explain (validated as read-only first)   |
| `database` | string  | yes      | Connection name                                              |
| `analyze`  | boolean | no       | Run EXPLAIN ANALYZE (PG only; ignored for CH / MySQL / MariaDB / SQLite, default false) |

Returns: query plan as text. The inner SQL is validated as read-only *before* EXPLAIN is prepended, which blocks the `EXPLAIN ANALYZE DELETE ...` bypass in PostgreSQL and MySQL 8.0.18+.

**Why `analyze=true` is ignored for MySQL/MariaDB:** MySQL 8.0.18+ `EXPLAIN ANALYZE` and MariaDB's `ANALYZE <stmt>` both execute the inner query (like PostgreSQL's EXPLAIN ANALYZE). Although the inner SQL is already validated as read-only so DELETE/UPDATE cannot slip through, we take a conservative stance: the MySQL/MariaDB backends run plain `EXPLAIN` regardless of the flag (strictly plan-only, no execution) and append a note to the output when the flag was set. Keep MySQL plan analysis at the SQL layer (`SELECT * FROM information_schema.statistics WHERE ...`) if deeper timing is needed.

**Why `analyze=true` is a no-op for SQLite:** SQLite's `EXPLAIN QUERY PLAN` is strictly plan-only — it never executes the inner query — so there's nothing useful to do with the `analyze` flag. We accept it for interface consistency and append a note when set.

### 11. `usage_guide`

Return a cheatsheet describing all tools, typical workflows, output formats, and schema-qualification rules. Use when unsure which tool to reach for.

| Parameter | Type | Required | Description |
| --------- | ---- | -------- | ----------- |
| (none)    |      |          |             |

Returns: a markdown guide covering the full tool surface plus the configured connections.

---

## SQL Validation — Reference Implementation

This is the critical security module. Whitelist approach, not blacklist.

```python
# src/readonly_db_mcp/validation.py
from sqlglot import parse, exp
from sqlglot.errors import ParseError

FORBIDDEN_NODES = (
    exp.Insert, exp.Update, exp.Delete,
    exp.Create, exp.Drop, exp.Alter,
    exp.TruncateTable, exp.Merge, exp.Command,
    exp.Set, exp.Copy, exp.Transaction,
    exp.Commit, exp.Rollback,
    exp.Into,  # SELECT ... INTO creates tables
)

ALLOWED_ROOT_TYPES = (
    exp.Select,
    exp.Union,       # SELECT ... UNION SELECT ...
    exp.Intersect,   # SELECT ... INTERSECT SELECT ...
    exp.Except,      # SELECT ... EXCEPT SELECT ...
)

def validate_read_only(sql: str, dialect: str) -> None:
    """
    Validates that a SQL string contains only read-only operations.
    Raises ValueError with a descriptive message if validation fails.
    """
    sql_stripped = sql.strip().rstrip(";")
    if not sql_stripped:
        raise ValueError("Empty query")

    try:
        statements = parse(sql_stripped, dialect=dialect)
    except ParseError as e:
        raise ValueError(f"SQL parse error: {e}")

    if not statements:
        raise ValueError("No valid SQL statements found")

    if len(statements) > 1:
        raise ValueError("Multiple statements not allowed — send one query at a time")

    ast = statements[0]

    # Whitelist: root must be SELECT-family
    if not isinstance(ast, ALLOWED_ROOT_TYPES):
        raise ValueError(
            f"Only SELECT queries are allowed. Got: {type(ast).__name__}"
        )

    # Walk full AST to catch writes hidden in CTEs or subqueries
    for node in ast.walk():
        if isinstance(node, FORBIDDEN_NODES):
            raise ValueError(
                f"Forbidden operation in query: {type(node).__name__}"
            )
```

### Validation Test Cases to Cover

**Should PASS:**

- `SELECT * FROM users`
- `SELECT a, b FROM t1 JOIN t2 ON t1.id = t2.id`
- `SELECT * FROM t1 WHERE id IN (SELECT id FROM t2)`
- `WITH cte AS (SELECT * FROM t1) SELECT * FROM cte`
- `SELECT count(*) FROM events GROUP BY date HAVING count(*) > 10`
- `SELECT *, ROW_NUMBER() OVER (PARTITION BY x ORDER BY y) FROM t`
- `SELECT * FROM t1 UNION ALL SELECT * FROM t2`
- `SELECT * FROM t1 EXCEPT SELECT * FROM t2`
- ClickHouse: `SELECT count() FROM events` (no args to count)
- ClickHouse: `SELECT * FROM events ARRAY JOIN tags`
- PostgreSQL: `SELECT id::text FROM users`
- PostgreSQL: `SELECT * FROM LATERAL (SELECT ...) sub`

**Should FAIL:**

- `INSERT INTO users VALUES (1, 'x')`
- `UPDATE users SET name = 'x'`
- `DELETE FROM users WHERE id = 1`
- `DROP TABLE users`
- `ALTER TABLE users ADD COLUMN x INT`
- `TRUNCATE TABLE users`
- `CREATE TABLE t (id INT)`
- `COPY users TO '/tmp/out'`
- `SET transaction_read_only = off`
- `SELECT 1; DROP TABLE users` (multi-statement)
- `WITH del AS (DELETE FROM users RETURNING *) SELECT * FROM del` (CTE with write)
- Empty string / whitespace only
- Invalid SQL syntax

---

## Query Execution Flow

```
AI sends SQL string
       |
       v
[1] validate_read_only(sql, dialect)     — sqlglot parse + AST whitelist check
       |                                    REJECT with clear message if not pure SELECT
       v
[2] acquire connection from pool
       |
       v
[3] execute in read-only context
       |   PG:  async with conn.transaction(readonly=True): await conn.fetch(sql)
       |   CH:  client.query(sql, settings={"readonly": "1"})
       v
[4] enforce row limit                    — truncate to MAX_RESULT_ROWS
       |                                    append "(showing N of M rows)" if truncated
       v
[5] format_as_markdown_table(columns, rows)
       |
       v
return string to AI agent
```

Errors at any step return a clear one-line message, never a stack trace.

---

## Server Skeleton — Reference Implementation

```python
# src/readonly_db_mcp/server.py
import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from mcp.server.fastmcp import FastMCP, Context

from .config import load_config
from .databases.postgres import PostgresBackend
from .databases.clickhouse import ClickHouseBackend
from .databases.base import DatabaseBackend
from .validation import validate_read_only
from .formatting import format_markdown_table


@dataclass
class AppContext:
    backends: dict[str, DatabaseBackend] = field(default_factory=dict)


@asynccontextmanager
async def lifespan(server: FastMCP):
    config = load_config()
    ctx = AppContext()

    # Initialize PG pools
    for pg in config.postgres_connections:
        backend = PostgresBackend(pg)
        await backend.connect()
        ctx.backends[pg.name] = backend

    # Initialize CH clients
    for ch in config.clickhouse_connections:
        backend = ClickHouseBackend(ch)
        await backend.connect()
        ctx.backends[ch.name] = backend

    try:
        yield ctx
    finally:
        for backend in ctx.backends.values():
            await backend.disconnect()


mcp = FastMCP("readonly-db-mcp", lifespan=lifespan)


def _get_backend(ctx: Context, name: str | None, db_type: str | None = None) -> DatabaseBackend:
    """Resolve a backend by name or return the first of the given type."""
    app: AppContext = ctx.request_context.lifespan_context
    if name:
        if name not in app.backends:
            raise ValueError(f"Unknown database: {name}. Available: {list(app.backends.keys())}")
        return app.backends[name]
    # Return first backend of the requested type
    for backend in app.backends.values():
        if db_type is None or backend.db_type == db_type:
            return backend
    raise ValueError(f"No {db_type or 'any'} database configured")


@mcp.tool()
async def query_postgres(sql: str, ctx: Context, database: str | None = None) -> str:
    """Execute a read-only SQL query against PostgreSQL. Returns results as a markdown table."""
    validate_read_only(sql, dialect="postgres")
    backend = _get_backend(ctx, database, db_type="postgres")
    columns, rows, total = await backend.execute(sql)
    return format_markdown_table(columns, rows, total)


@mcp.tool()
async def query_clickhouse(sql: str, ctx: Context, database: str | None = None) -> str:
    """Execute a read-only SQL query against ClickHouse. Returns results as a markdown table."""
    validate_read_only(sql, dialect="clickhouse")
    backend = _get_backend(ctx, database, db_type="clickhouse")
    columns, rows, total = await backend.execute(sql)
    return format_markdown_table(columns, rows, total)


@mcp.tool()
async def list_databases(ctx: Context) -> str:
    """List all configured database connections."""
    app: AppContext = ctx.request_context.lifespan_context
    lines = []
    for name, backend in app.backends.items():
        lines.append(f"- **{name}** ({backend.db_type}) — {backend.host}/{backend.database}")
    return "\n".join(lines) if lines else "No databases configured."


@mcp.tool()
async def list_tables(database: str, ctx: Context, schema: str | None = None) -> str:
    """List all tables in a configured database (optionally scoped to a schema)."""
    backend = _get_backend(ctx, database)
    if schema is not None:
        validate_identifier(schema)
    tables = await backend.list_tables(schema=schema)
    return "\n".join(f"- {t}" for t in tables) if tables else "No tables found."


@mcp.tool()
async def describe_table(database: str, table: str, ctx: Context) -> str:
    """Describe columns, types, and nullability for a table."""
    backend = _get_backend(ctx, database)
    columns = await backend.describe_table(table)
    return format_markdown_table(
        ["column", "type", "nullable"],
        [(c["name"], c["type"], c["nullable"]) for c in columns],
        len(columns),
    )


@mcp.tool()
async def explain_query(sql: str, database: str, ctx: Context, analyze: bool = False) -> str:
    """Show the execution plan for a query. Set analyze=True to run EXPLAIN ANALYZE."""
    validate_read_only(sql, dialect="postgres")  # validate the inner query
    backend = _get_backend(ctx, database)
    plan = await backend.explain(sql, analyze=analyze)
    return plan


def main():
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
```

---

## Database Backend Interface

```python
# src/readonly_db_mcp/databases/base.py
from abc import ABC, abstractmethod


class DatabaseBackend(ABC):
    db_type: str   # "postgres" or "clickhouse"
    host: str
    database: str

    @abstractmethod
    async def connect(self) -> None: ...

    @abstractmethod
    async def disconnect(self) -> None: ...

    @abstractmethod
    async def execute(self, sql: str) -> tuple[list[str], list[tuple], int]:
        """Returns (column_names, rows, total_row_count_before_truncation)."""
        ...

    @abstractmethod
    async def list_tables(self, schema: str | None = None) -> list[str]: ...

    @abstractmethod
    async def describe_table(self, table: str) -> list[dict]: ...

    @abstractmethod
    async def explain(self, sql: str, analyze: bool = False) -> str: ...
```

---

## PostgreSQL Backend Notes

```python
# Key implementation details for src/readonly_db_mcp/databases/postgres.py

# Connection pool with read-only enforced at connection level:
pool = await asyncpg.create_pool(
    dsn=dsn,
    min_size=1,
    max_size=5,
    server_settings={"default_transaction_read_only": "on"},
)

# Query execution — explicit read-only transaction:
async with pool.acquire() as conn:
    async with conn.transaction(readonly=True):
        rows = await conn.fetch(sql, timeout=query_timeout)

# list_tables query:
"""
SELECT schemaname || '.' || tablename AS table_name
FROM pg_tables
WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
ORDER BY schemaname, tablename
"""

# describe_table query (for schema.table input):
"""
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_schema = $1 AND table_name = $2
ORDER BY ordinal_position
"""

# explain:
f"EXPLAIN {'ANALYZE ' if analyze else ''}{sql}"
# EXPLAIN ANALYZE is safe in a read-only transaction — PG rolls back after
```

---

## ClickHouse Backend Notes

```python
# Key implementation details for src/readonly_db_mcp/databases/clickhouse.py

# Client init with readonly enforced:
client = clickhouse_connect.get_client(
    host=host, port=port,
    username=user, password=password,
    database=database,
    settings={"readonly": "1"},
)

# Query execution:
result = client.query(sql, settings={"readonly": "1", "max_execution_time": timeout})
columns = result.column_names
rows = result.result_rows

# list_tables query:
"SHOW TABLES"

# describe_table query:
f"DESCRIBE TABLE {table}"

# explain:
f"EXPLAIN {sql}"

# IMPORTANT: clickhouse-connect is synchronous. Wrap in asyncio.to_thread():
rows = await asyncio.to_thread(client.query, sql, settings={...})
```

---

## Config Module Notes

```python
# src/readonly_db_mcp/config.py
# Use pydantic-settings to parse env vars.
# Pattern: PG_1_NAME, PG_1_HOST, ... PG_2_NAME, PG_2_HOST, ...
# Same for CH_1_, CH_2_, etc.
# Dynamically discover numbered connections from the environment.

import os
from dataclasses import dataclass


@dataclass
class PostgresConnection:
    name: str
    host: str
    port: int
    database: str
    user: str
    password: str


@dataclass
class ClickHouseConnection:
    name: str
    host: str
    port: int
    database: str
    user: str
    password: str


@dataclass
class Config:
    postgres_connections: list[PostgresConnection]
    clickhouse_connections: list[ClickHouseConnection]
    query_timeout_seconds: int = 30
    max_result_rows: int = 1000


def load_config() -> Config:
    """
    Scan env vars for PG_N_* and CH_N_* patterns using regex.
    Gaps are allowed: PG_1_ and PG_3_ work even if PG_2_ is missing.
    """
    # Scan all env vars for PG_N_NAME patterns to find unique IDs
    pg_ids = sorted({int(m.group(1)) for k in os.environ if (m := re.match(r"^PG_(\d+)_NAME$", k))})
    pg_conns = []
    for i in pg_ids:
        context = f"PG_{i}"
        pg_conns.append(PostgresConnection(
            name=_require_env(f"PG_{i}_NAME", context),
            host=_require_env(f"PG_{i}_HOST", context),
            port=int(os.environ.get(f"PG_{i}_PORT", "5432")),
            database=_require_env(f"PG_{i}_DATABASE", context),
            user=_require_env(f"PG_{i}_USER", context),
            password=_require_env(f"PG_{i}_PASSWORD", context),
        ))

    # Same for CH
    ch_ids = sorted({int(m.group(1)) for k in os.environ if (m := re.match(r"^CH_(\d+)_NAME$", k))})
    ch_conns = []
    for i in ch_ids:
        context = f"CH_{i}"
        ch_conns.append(ClickHouseConnection(
            name=_require_env(f"CH_{i}_NAME", context),
            host=_require_env(f"CH_{i}_HOST", context),
            port=int(os.environ.get(f"CH_{i}_PORT", "8123")),
            database=_require_env(f"CH_{i}_DATABASE", context),
            user=_require_env(f"CH_{i}_USER", context),
            password=_require_env(f"CH_{i}_PASSWORD", context),
        ))

    if not pg_conns and not ch_conns:
        raise ValueError(
            "No database connections configured. "
            "Set PG_1_NAME/PG_1_HOST/... or CH_1_NAME/CH_1_HOST/... environment variables."
        )

    return Config(
        postgres_connections=pg_conns,
        clickhouse_connections=ch_conns,
        query_timeout_seconds=int(os.environ.get("QUERY_TIMEOUT_SECONDS", "30")),
        max_result_rows=int(os.environ.get("MAX_RESULT_ROWS", "1000")),
    )
```

---

## Formatting Module Notes

The formatter supports three output formats selectable per-query via
`output_format` on `query_postgres` / `query_clickhouse` / `sample_table`:

| Format      | Use when                                                          | Cell truncation |
| ----------- | ----------------------------------------------------------------- | --------------- |
| `table`     | Many rows with short values (default)                             | Yes, at 200 chars |
| `vertical`  | Wide values (DDL strings, JSON blobs) or single-row inspection    | None            |
| `json`      | Programmatic post-processing by the AI                            | None            |

Auto-switch rule: when `output_format="table"` is requested but the result is
a single row with at least one cell exceeding the truncation limit, the
formatter silently falls back to `vertical` so the AI gets the full value
instead of a truncated `...`. When any cell in a multi-row table is truncated,
a note is appended suggesting the AI retry with `vertical` or `json`.

Zero-row handling: previously, queries returning zero rows produced the
misleading "Query returned no columns" message. The formatter now emits an
empty markdown table with headers plus "*Query returned 0 rows.*", which
clearly distinguishes "the schema is X and there are no matching rows" from
an actual empty column set.

Rendering rules shared across formats:

- None/NULL values render as the literal `NULL`
- In `table`, pipes/newlines are escaped; values >200 chars are truncated
- In `vertical` and `json`, values are rendered in full
- JSON non-primitive types (Decimal, datetime, UUID, bytes) are coerced via
  `_json_safe`; bytes decode as UTF-8 when possible, else hex

---

## pyproject.toml

```toml
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[project]
name = "readonly-db-mcp"
version = "0.1.0"
description = "Read-only MCP server for PostgreSQL and ClickHouse — safe database access for AI agents"
readme = "README.md"
license = "MIT"
requires-python = ">=3.11"
authors = [
    { name = "readonly-db-mcp contributors" },
]
keywords = ["mcp", "postgresql", "clickhouse", "read-only", "ai", "claude"]
classifiers = [
    "Development Status :: 3 - Alpha",
    "Intended Audience :: Developers",
    "License :: OSI Approved :: MIT License",
    "Programming Language :: Python :: 3.11",
    "Programming Language :: Python :: 3.12",
    "Programming Language :: Python :: 3.13",
]
dependencies = [
    "mcp>=1.2.0",
    "sqlglot>=26.0",
    "asyncpg>=0.29",
    "clickhouse-connect>=0.7",
    "python-dotenv>=1.0",
]

[project.scripts]
readonly-db-mcp = "readonly_db_mcp.server:main"

[project.optional-dependencies]
dev = [
    "pytest>=8.0",
    "pytest-asyncio>=0.24",
    "ruff>=0.8",
]

[tool.hatch.build.targets.wheel]
packages = ["src/readonly_db_mcp"]

[tool.pytest.ini_options]
testpaths = ["tests"]
asyncio_mode = "auto"

[tool.ruff]
target-version = "py311"
line-length = 120
```

---

## Design Decisions

### Why whitelist validation instead of blacklist?

A blacklist approach would need to enumerate every dangerous SQL keyword and operation. If a new SQL feature is added or a bypass is discovered, the blacklist would miss it. The whitelist approach only allows known-safe operations (`SELECT`, `UNION`, `INTERSECT`, `EXCEPT`) and rejects everything else by default. Unknown operations are automatically blocked.

### Why three layers of write protection?

Each layer alone has known bypasses:
- **Layer 1 (SQL parsing)** cannot detect side effects inside stored procedures or database functions. `SELECT my_write_function()` looks like a pure SELECT to the AST parser.
- **Layer 2 (connection-level read-only)** can be overridden by `SET` commands in some configurations (Layer 1 blocks these).
- **Layer 3 (DB user privileges)** depends on the operator configuring it correctly. If misconfigured, Layers 1 and 2 still protect the database.

Together, they provide defense-in-depth. All three layers must fail for a write to reach production.

### Why python-dotenv instead of pydantic-settings?

Simplicity. This tool has very simple configuration needs (flat env vars with defaults). python-dotenv provides `.env` file loading with zero boilerplate. pydantic-settings adds validation overhead and complexity that isn't needed here — the env var parsing is trivial enough to do manually with clear error messages.

### Why inject LIMIT via AST manipulation instead of subquery wrapping?

Earlier versions wrapped user queries as `SELECT * FROM ({user_sql}) AS _limited_query LIMIT N`. This breaks `ORDER BY` semantics — PostgreSQL does not guarantee that ordering from an inner subquery propagates to the outer query. The AST-based approach modifies the user's query directly to add/tighten the LIMIT clause, preserving all other semantics including ORDER BY.

### Why reject SHOW / DESCRIBE / EXPLAIN / EXISTS instead of allowing them?

Users of the tool often try raw `SHOW TABLES`, `DESCRIBE t`, or `EXPLAIN ...`
as SQL via `query_*`. These get parsed by sqlglot as `exp.Command` and
rejected. The ergonomic fix would be to allow a small whitelist of these
commands — but at least one of them is a genuine write-bypass vector:

- PostgreSQL's `EXPLAIN ANALYZE` **actually executes** the inner query. An
  allowlist entry for "EXPLAIN" would accept `EXPLAIN ANALYZE DELETE FROM
  users` and — without careful inner-query parsing — pass it to the database
  inside a read-only transaction. The PG-level read-only block would catch
  the DELETE in this specific case, but we don't want to rely on a single
  layer when the whole design is defense-in-depth.

- `SHOW`/`DESCRIBE`/`EXISTS` are individually safe, but making the validator
  command-aware creates maintenance burden (tracking each dialect's command
  surface) for little benefit, since we already expose dedicated tools for
  each one.

Instead, the validator produces a pointed error message when it detects one
of these commands as the root, suggesting the correct dedicated tool. This
preserves the whitelist security model while closing the discoverability gap.

### Why separate `MYSQL_` and `MARIADB_` prefixes if the backend class is shared?

MariaDB forked from MySQL and is wire-protocol-compatible — one driver (`asyncmy`) handles both, and sqlglot's `mysql` dialect parses both for SELECT-shape validation. Mechanically, a single config + backend would suffice.

We keep them separate at the config layer because:

1. **Explicitness at operator config time.** "Is this MySQL or MariaDB?" affects timeout semantics (`max_execution_time` in ms for MySQL vs `max_statement_time` in seconds for MariaDB), SHOW statement variants, and some recovery commands. Making operators opt in explicitly avoids the "works until it doesn't" class of bugs when timeout behavior silently differs.
2. **Distinct labels in `list_databases`.** AIs get a clearer picture of the environment when `prod_mysql (mysql)` and `legacy (mariadb)` are visibly separate.
3. **Per-flavor tool paths.** `query_mysql` and `query_mariadb` are distinct tools. Route-by-flavor at the tool layer lets us tighten each independently in the future (e.g., adding MariaDB-specific `table_stats` fields from `information_schema.INNODB_SYS_TABLESTATS`).

The two backends share an implementation (`_MySQLFamilyBackend` base class) but are exposed as distinct concrete classes (`MySQLBackend`, `MariaDBBackend`), each with its own class-level `db_type` attribute. This matches the pattern in `postgres.py` and `clickhouse.py` (each of those also declares `db_type` at class scope) and means `_get_backend`'s type check sees a real class-level value rather than a per-instance attribute shadow. The shared base class carries the asyncmy pool management, transaction wrapping, identifier validation, and `information_schema` queries; the subclasses only override `db_type`, and the timeout-prelude branches on `self.db_type` at runtime.

### Why per-query `START TRANSACTION READ ONLY` on MySQL/MariaDB instead of trusting the session flag?

MySQL's session-level `SET SESSION TRANSACTION READ ONLY` (which we set via asyncmy's `init_command`) applies to all subsequent transactions, but a pooled connection could in theory be asked to toggle it back to READ WRITE mid-session. Wrapping each query in its own explicit `START TRANSACTION READ ONLY` / `COMMIT` means:

- Every query runs inside a named read-only transaction — no ambiguity
- The connection returns to the pool in a clean state (no long-running implicit transactions holding locks)
- Same mental model as the PostgreSQL backend's `conn.transaction(readonly=True)` wrapping

The COMMIT at the end is a no-op on the server for read-only transactions, so the overhead is small.

### Why MySQL/MariaDB `EXPLAIN ANALYZE` is not exposed even with `analyze=True`?

MySQL 8.0.18+ and MariaDB's `ANALYZE <stmt>` both execute the inner query. For PostgreSQL we accept this because the outer read-only transaction blocks any write side effects (EXPLAIN ANALYZE can time a DELETE inside a read-only transaction, which is a rollback). For MySQL/MariaDB we take a stricter stance and always run plain `EXPLAIN` — even though our outer read-only transaction would also roll back any writes, the extra defense-in-depth here costs nothing (you can still read the execution plan) and removes any chance that a future MySQL bug, a misconfigured connection, or a stored procedure with `SECURITY DEFINER` privileges lets an ANALYZE-time side effect commit.

### Why use stdlib `sqlite3` for SQLite instead of an async driver like `aiosqlite`?

Two reasons:

1. **No new dependency.** `sqlite3` is in the Python stdlib. `aiosqlite` is a thin wrapper that still uses `sqlite3` under the hood — it spawns a dedicated thread per connection to run the blocking calls. We can achieve the same by wrapping `sqlite3` calls in `asyncio.to_thread()`, which is exactly what the ClickHouse backend already does (for `clickhouse-connect`). Pattern consistency over a new dep.
2. **SQLite queries are typically fast.** The overhead of `asyncio.to_thread()` (thread hop, ~tens of microseconds) is insignificant for most SQLite workloads. For heavy SQLite use cases (e.g., analytical queries on large .db files), users are probably on DuckDB anyway.

### SQLite's unique trust boundary: filesystem paths

Every other backend treats "who can see what" as a database-user question (GRANT SELECT ON schema). SQLite has no GRANT — the database is a file, and filesystem permissions are the authorization model. This means **operators must treat `SQLITE_N_PATH` env vars the way they treat GRANT statements on other backends**: each configured path is a bundle of capabilities, and misconfiguration (e.g., pointing at `/etc/passwd` — SQLite rejects it as "not a database", but still an OOPS) is an operator concern.

Things we actively do to limit blast radius within this model:

1. Open files via `file:<path>?mode=ro` (VFS-level read-only). Main DB is immutable.
2. `enable_load_extension(False)` so `SELECT load_extension(...)` can't load arbitrary .so files and bypass everything.
3. Reject `ATTACH DATABASE` at the validator level. Without this, a caller could attach a file the operator didn't configure (the attached DB would be read-only too, but still widens the operator's stated reachable-path set).
4. Reject `PRAGMA writable_schema=ON` (sqlglot parses `PRAGMA` as `exp.Command`, rejected at the root).

### Why asyncpg for PostgreSQL but not an async ClickHouse client?

asyncpg is a mature, battle-tested async PostgreSQL driver with connection pooling and health checks built-in. ClickHouse's official Python client (`clickhouse-connect`) is synchronous and uses HTTP under the hood (no persistent connections). We wrap all `clickhouse-connect` calls in `asyncio.to_thread()` to avoid blocking the event loop. This is pragmatic — the overhead is acceptable, and the alternative (using an unofficial async client) introduces more risk.

### Why stdio transport only?

Claude Code (the primary use case) communicates with MCP servers over stdio. HTTP/SSE transport adds complexity (authentication, TLS, deployment) without immediate value. For multi-client scenarios, operators can run multiple server instances with different env configs.

### Why graceful degradation on startup?

In multi-database setups (e.g., prod + staging + analytics), a single database being temporarily unreachable should not block the entire server. The server starts with whatever databases connect successfully and logs failures. This improves operational resilience — the AI can still query available databases while operators fix the failing ones.

### Why no query cost estimation or rate limiting?

Query cost estimation is database-specific and complex to implement correctly. Rate limiting requires per-user tracking and state management. Both add significant complexity. For v0.1, the mitigation strategy is:
- Conservative timeouts (`QUERY_TIMEOUT_SECONDS`)
- Row limits (`MAX_RESULT_ROWS`)
- Database-level resource constraints (`statement_mem`, `max_memory_usage`)
- Using read replicas instead of primary databases

Operators who need stricter controls can add them at the infrastructure level (reverse proxy, resource quotas, etc.).

---

## Known Limitations (v0.1)

- **No rate limiting or query cost estimation** — see README.md for mitigations
- **No schema/table filtering** — AI can see all tables the DB user can access
- **No row-level security** — handled at DB user/role level
- **No query result caching** — every query hits the database
- **Stored procedures can bypass validation** — Layer 3 (DB permissions) is mandatory
- **stdio transport only** — no HTTP/SSE support

---

## DB User Setup (reference for operators)

The MCP server assumes these read-only users exist. This is a one-time infra task, not handled by this tool.

### PostgreSQL

```sql
CREATE ROLE ai_reader LOGIN PASSWORD 'strong_password_here';
GRANT CONNECT ON DATABASE mydb TO ai_reader;
GRANT USAGE ON SCHEMA public TO ai_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO ai_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO ai_reader;
-- PG 14+: GRANT pg_read_all_data TO ai_reader;
```

### ClickHouse

```sql
CREATE USER ai_reader IDENTIFIED BY 'strong_password_here'
    SETTINGS PROFILE 'readonly';
GRANT SELECT ON mydb.* TO ai_reader;
```

### MySQL

```sql
CREATE USER 'ai_reader'@'%' IDENTIFIED BY 'strong_password_here';
GRANT SELECT ON myapp.* TO 'ai_reader'@'%';
-- information_schema is globally readable for the user's own objects by default;
-- no extra grant needed for describe_table / list_tables metadata queries.
```

Note: do **not** grant `SHOW VIEW`, `EXECUTE`, `TRIGGER`, or any DML privileges. The AI never needs these, and granting `EXECUTE` on stored functions is especially risky because `SELECT my_write_function()` parses as a pure SELECT but can perform writes via function body.

`FLUSH PRIVILEGES` is deliberately omitted. It's only needed when you modify the `mysql.user` table directly (e.g., `UPDATE mysql.user SET ...`). `CREATE USER` and `GRANT` update the in-memory privilege cache automatically on MySQL 5.7+ and MariaDB 10+, so the flush is a cargo-culted no-op here.

### MariaDB

```sql
CREATE USER 'ai_reader'@'%' IDENTIFIED BY 'strong_password_here';
GRANT SELECT ON legacy_app.* TO 'ai_reader'@'%';
```

Same privilege posture as MySQL. MariaDB's `SHOW GRANTS FOR 'ai_reader'@'%'` should show only `GRANT SELECT ON legacy_app.*` and `GRANT USAGE ON *.*` (USAGE is implicit connect).

### SQLite

SQLite has no GRANT system. Authorization is filesystem-level:

1. **Use a dedicated unix user for the MCP server process.** That user should have `r--` (read-only) access to the `.db` file and `r-x` on the directory that contains it. Don't give the MCP process write access "just in case" — the VFS read-only URI is belt, but filesystem permissions are suspenders.
2. **Put `.db` files on their own path prefix you can audit.** e.g. `/var/data/mcp-readonly/*.db` — one glob you can point an auditing tool at, and the operator knows at a glance which databases are reachable.
3. **Do not configure the MCP server with a path that contains symlinks pointing out of that prefix.** SQLite follows symlinks silently.
4. **Never configure a SQLite path that the MCP process itself writes to.** If the DB is shared with another process that writes, the MCP server's queries may observe inconsistent state (SQLite WAL mode handles concurrent readers/writers correctly, but from an operations standpoint, feeding the AI live-updating data is rarely what you want — use a snapshot instead).

---

## Operational Considerations

### Logging

The server uses Python's standard `logging` module with structured log messages. All logs go to stderr (stdout is reserved for the MCP protocol). Operators can control verbosity with the `LOGLEVEL` environment variable:

```bash
LOGLEVEL=DEBUG readonly-db-mcp   # Full query and connection tracing
LOGLEVEL=INFO readonly-db-mcp    # Connection lifecycle events (default for monitoring)
LOGLEVEL=WARNING readonly-db-mcp # Only problems (default)
```

Key events logged:
- Connection establishment and failures (INFO)
- Query execution with truncated SQL (DEBUG)
- Validation errors (INFO)
- Execution failures with full exception (ERROR)
- Graceful degradation on startup (WARNING)
- Reconnection attempts (WARNING)

### Connection Health

- **PostgreSQL**: asyncpg's connection pool automatically validates connections on acquire. Stale connections are detected and replaced transparently.
- **ClickHouse**: Queries that fail with connection/network errors trigger automatic reconnection (one retry). The client is replaced with a fresh instance.

### Graceful Degradation

If configured with multiple databases (e.g., prod + staging + analytics), the server starts successfully as long as at least one database connects. Failed connections are logged but don't block startup. This improves resilience — operators can fix failing databases without downtime for the working ones.

### Resource Limits

- **Query timeout**: `QUERY_TIMEOUT_SECONDS` (default 30s) enforced both client-side and server-side
- **Result size**: `MAX_RESULT_ROWS` (default 1000) enforced via LIMIT injection
- **Connection pool**: PostgreSQL uses min=1, max=5 connections per backend
- **Memory**: No explicit limit — relies on LIMIT injection to cap result size

For production deployments, also configure database-level resource limits:
- PostgreSQL: `statement_mem`, `work_mem`, `statement_timeout`
- ClickHouse: `max_memory_usage`, `max_execution_time`

### Security Checklist

- [ ] Database users have only `SELECT` privileges
- [ ] Database users do not own any tables
- [ ] Database users cannot execute write-capable stored procedures
- [ ] Use dedicated read replicas (not primary databases)
- [ ] Set conservative `QUERY_TIMEOUT_SECONDS` (e.g., 10-30s)
- [ ] Monitor database load and set resource limits
- [ ] Review logs regularly for failed validation attempts
- [ ] Keep sqlglot updated (new SQL features may need validation rules)

---

## Research and Background

This design was informed by research into SQL injection bypasses, MCP security, and production incidents:

- **sqlglot** chosen over sqlparse (tokenizer only) and pglast (PG-only) because it supports both PG and CH dialects with the same API and provides full AST access
- **Whitelist validation** chosen over blacklist because new SQL features/keywords could bypass a blacklist — the safest approach is to only allow known-good operations
- **Three-layer defense** because each layer alone has known bypasses (documented above in "Design Decisions")
- The official ClickHouse MCP server had a critical vulnerability where `readonly=1` was bypassed and an AI agent dropped a production table — this motivated the defense-in-depth approach
- Testing found that subquery wrapping breaks ORDER BY semantics in PostgreSQL, leading to the AST-based LIMIT injection approach
