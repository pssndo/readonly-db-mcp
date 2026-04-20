# CLAUDE.md

## Project

`readonly-db-mcp` — a pip-installable MCP server that gives AI agents read-only SQL access to PostgreSQL, ClickHouse, MySQL, and MariaDB. Three layers of write protection: sqlglot AST validation, connection-level read-only enforcement, and DB user permissions.

## Architecture

See `ARCHITECTURE.md` for the full system design, reference implementations, validation test cases, and operational considerations. The project is fully implemented — focus on maintenance, bug fixes, and enhancements.

## Tech

- Python >=3.11
- `mcp` (FastMCP) for the MCP server
- `sqlglot` for SQL parsing/validation (dialects: `postgres`, `clickhouse`, `mysql` — MariaDB uses the mysql dialect)
- `asyncpg` for PostgreSQL, `clickhouse-connect` for ClickHouse, `asyncmy` for MySQL + MariaDB
- `python-dotenv` for .env file loading
- `pytest` + `pytest-asyncio` for tests

## Structure

```
src/readonly_db_mcp/
  __init__.py
  server.py          # FastMCP server, tool definitions, lifespan
  config.py          # Env var parsing (PG_1_*, CH_1_*, MYSQL_1_*, MARIADB_1_*)
  validation.py      # sqlglot read-only validator (whitelist, not blacklist)
  formatting.py      # Results -> markdown tables / vertical / json envelope
  databases/
    base.py          # Abstract DatabaseBackend interface
    postgres.py      # asyncpg, read-only transactions
    clickhouse.py    # clickhouse-connect, readonly=1
    mysql.py         # asyncmy, SESSION READ ONLY + START TRANSACTION READ ONLY
                     # Shared by MySQL and MariaDB; flavor="mysql"|"mariadb"
                     # selects the timeout session variable
tests/
  test_validation.py # SQL validation tests (security-critical)
  test_config.py
  test_formatting.py
  test_backends.py   # Backend unit tests (identifier validation, LIMIT injection)
  test_server.py
```

## Commands

```bash
pip install -e ".[dev]"        # Install with dev deps
pytest                         # Run tests
pytest tests/test_validation.py  # Run just validation tests
ruff check src/ tests/         # Lint
ruff format src/ tests/        # Format
readonly-db-mcp                # Run the server (needs env vars)
```

## Key rules

- The SQL validator uses a **whitelist** approach: root AST node must be SELECT/UNION/INTERSECT/EXCEPT. Everything else is rejected. The full AST is walked to catch writes hidden in CTEs or subqueries.
- `clickhouse-connect` is synchronous — always wrap calls in `asyncio.to_thread()`. `asyncpg` and `asyncmy` are native-async.
- MySQL/MariaDB Layer 2: set `SET SESSION TRANSACTION READ ONLY` via asyncmy's `init_command`, and wrap every query in `START TRANSACTION READ ONLY` / `COMMIT`. No per-connection server-enforced flag like PG — the explicit per-query transaction is doing the real work.
- MySQL 8.0.18+ `EXPLAIN ANALYZE` and MariaDB's `ANALYZE <stmt>` both **execute** the inner query. The MySQL backend's `explain()` always runs plain `EXPLAIN` (plan-only) regardless of the `analyze` flag; add a note to the output if `analyze=True` was requested.
- Never return stack traces to the AI agent. Catch exceptions and return clear one-line error messages (with forwarded driver detail via `_safe_error_message`).
- Results are formatted as markdown tables (default), vertical key=value rows, or strict JSON envelope. Truncate to `MAX_RESULT_ROWS` and note when truncated.
- The server runs on stdio transport only (what Claude Code uses).

## Style

- Type hints on all function signatures
- Docstrings on public functions
- `ruff` for linting and formatting (line length 120)
- Tests use `pytest-asyncio` with `asyncio_mode = "auto"`
