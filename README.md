# readonly-db-mcp

Read-only MCP server for PostgreSQL, ClickHouse, MySQL, and MariaDB — safe database access for AI agents.

## What is this?

This is an [MCP](https://modelcontextprotocol.io/) (Model Context Protocol) server that lets AI agents like Claude Code run SQL queries against your databases. The key constraint: **it only allows read-only queries**. The AI can look at your data but can never modify it.

MCP is a standard protocol that AI tools use to call external functions ("tools"). This server exposes tools like `query_postgres`, `query_mysql`, and `list_tables` that the AI discovers automatically. The AI sends a SQL query, this server validates it, runs it, and returns the results as a markdown table.

## How it keeps your data safe

Three independent layers of write protection. Each layer alone is sufficient to block writes; all three must fail for a write to reach your database.

```
AI Agent sends SQL
       |
       v
[Layer 1] sqlglot AST validation
       |   Parse SQL into a syntax tree. Only allow SELECT at the root.
       |   Walk the full tree to catch writes hidden in CTEs/subqueries.
       |   REJECT anything that isn't a pure read operation.
       |
       v
[Layer 2] Connection-level read-only enforcement
       |   PostgreSQL:     default_transaction_read_only = on
       |   ClickHouse:     readonly = 1
       |   MySQL/MariaDB:  SET SESSION TRANSACTION READ ONLY (at connect)
       |                   + START TRANSACTION READ ONLY per query
       |   Even if Layer 1 has a bug, the database itself rejects writes.
       |
       v
[Layer 3] Database user permissions (set up by you)
       |   The DB user should only have SELECT privileges.
       |   Even if Layers 1 and 2 fail, the DB rejects unauthorized writes.
       |
       v
  Results returned to AI as a markdown table
```

**Why three layers?** Each layer alone has known bypasses:
- Layer 1 (SQL parsing) can't detect side effects in stored procedures
- Layer 2 (PG `default_transaction_read_only`, MySQL/MariaDB `SESSION TRANSACTION READ ONLY`) can be overridden by `SET` (which Layer 1 blocks). For MySQL/MariaDB Layer 2 is also slightly weaker than PG's because there's no server-enforced pool-level flag — we compensate by wrapping every query in `START TRANSACTION READ ONLY` explicitly.
- Layer 3 (DB user privileges) depends on the operator configuring it correctly

Together, they provide defense in depth.

**MySQL/MariaDB `EXPLAIN ANALYZE` warning.** In MySQL 8.0.18+ and MariaDB, `EXPLAIN ANALYZE <stmt>` and MariaDB's `ANALYZE <stmt>` **actually execute** the inner query to collect timing data (same semantics as PostgreSQL's `EXPLAIN ANALYZE`). This tool's `explain_query` always runs plain `EXPLAIN` (plan-only, no execution) for MySQL and MariaDB, regardless of the `analyze` flag, and rejects raw `EXPLAIN ANALYZE ...` sent via `query_mysql` / `query_mariadb` at the SQL validation layer.

### Important: Layer 3 is not optional

The SQL validator (Layer 1) cannot detect side effects hidden inside stored procedures or functions. For example, `SELECT my_write_function()` looks like a pure SELECT to the AST parser, but the function could execute arbitrary writes internally. Layer 2 (connection-level read-only) catches many of these cases in PostgreSQL, but not all (e.g., functions declared with `SECURITY DEFINER` or using `dblink`).

**Layer 3 (DB user permissions) is your last line of defense.** The database user configured for this server must:
- Have only `SELECT` privileges (no `INSERT`, `UPDATE`, `DELETE`, `CREATE`, etc.)
- Not own any tables (owners can always modify their tables)
- Not have `EXECUTE` privileges on functions that perform writes
- For PostgreSQL 14+, use `GRANT pg_read_all_data` for clean read-only access

See [Database user setup](#database-user-setup) below for the exact SQL commands.

## Project structure

```
src/readonly_db_mcp/
  __init__.py          # Package version
  server.py            # MCP server entry point, tool definitions, startup/shutdown
  config.py            # Reads database connection settings from env vars / .env
  validation.py        # SQL validator — parses queries and rejects non-SELECT
  formatting.py        # Converts query results to markdown tables
  databases/
    base.py            # Abstract interface that all DB backends implement
    postgres.py        # PostgreSQL backend (asyncpg, connection pool, read-only txns)
    clickhouse.py      # ClickHouse backend (clickhouse-connect, readonly=1)
    mysql.py           # MySQL + MariaDB backend (asyncmy pool, SESSION READ ONLY + START TRANSACTION READ ONLY)
tests/
  test_validation.py   # SQL validation tests (the critical security tests)
  test_config.py       # Config parsing tests
  test_formatting.py   # Result formatting tests
  test_server.py       # MCP tool-level tests (with mocked DB backends)
```

## Development setup

### Prerequisites

- **Python 3.11+** (required — uses 3.11+ syntax features)
- **pip** and **venv** (`sudo apt install python3.11 python3.11-venv python3-pip` on Ubuntu/WSL)

### Install

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

### Run tests

```bash
pytest                           # all tests
pytest tests/test_validation.py  # just validation tests (the security-critical ones)
```

### Lint / format

```bash
ruff check src/ tests/           # check for lint errors
ruff format src/ tests/          # auto-format code
```

### Run the server

```bash
readonly-db-mcp    # needs env vars configured — see Configuration below
```

## Configuration

Set database connections via environment variables or a `.env` file in the working directory. Multiple databases are supported via numbered prefixes.

### Environment variables

```env
# PostgreSQL connections (PG_1_, PG_2_, etc.)
PG_1_NAME=prod_db               # Friendly name — the AI uses this to pick a database
PG_1_HOST=pg-prod.internal      # Hostname
PG_1_PORT=5432                  # Port (optional, defaults to 5432)
PG_1_DATABASE=myapp             # Database name
PG_1_USER=ai_reader             # Username (should be a read-only DB user)
PG_1_PASSWORD=secret            # Password

# ClickHouse connections (CH_1_, CH_2_, etc.)
CH_1_NAME=analytics
CH_1_HOST=ch-prod.internal
CH_1_PORT=8123                  # HTTP port (optional, defaults to 8123)
CH_1_DATABASE=events
CH_1_USER=ai_reader
CH_1_PASSWORD=secret

# MySQL connections (MYSQL_1_, MYSQL_2_, etc.)
MYSQL_1_NAME=primary_mysql
MYSQL_1_HOST=mysql-prod.internal
MYSQL_1_PORT=3306               # optional, defaults to 3306
MYSQL_1_DATABASE=myapp
MYSQL_1_USER=ai_reader
MYSQL_1_PASSWORD=secret

# MariaDB connections (MARIADB_1_, MARIADB_2_, etc.)
# Separate prefix from MySQL. Same wire protocol, but kept distinct so
# operators can be explicit — and so query_mariadb uses MariaDB-specific
# timeout semantics (max_statement_time in seconds vs MySQL's max_execution_time in ms).
MARIADB_1_NAME=legacy
MARIADB_1_HOST=mariadb-prod.internal
MARIADB_1_PORT=3306             # optional, defaults to 3306
MARIADB_1_DATABASE=legacy_app
MARIADB_1_USER=ai_reader
MARIADB_1_PASSWORD=secret

# Global settings (optional)
QUERY_TIMEOUT_SECONDS=30        # Max seconds per query (default: 30)
MAX_RESULT_ROWS=1000            # Max rows returned (default: 1000, rest truncated)
```

Numbering starts from 1 and gaps are allowed. If PG_1_ and PG_3_ exist but PG_2_ is missing, both are loaded.

### .env file

If a `.env` file exists in the working directory, it is loaded automatically. Environment variables set in the shell or MCP config take priority over `.env` values.

## Claude Code setup

```bash
pip install readonly-db-mcp
```

Add to `.claude/mcp.json` in your project:

```json
{
  "mcpServers": {
    "readonly-db": {
      "command": "readonly-db-mcp",
      "env": {
        "PG_1_NAME": "my_postgres",
        "PG_1_HOST": "localhost",
        "PG_1_PORT": "5432",
        "PG_1_DATABASE": "mydb",
        "PG_1_USER": "ai_reader",
        "PG_1_PASSWORD": "secret"
      }
    }
  }
}
```

Claude Code auto-discovers the tools. No further setup needed.

## MCP tools

Ten tools are exposed to the AI agent:

| Tool | Parameters | Description |
|------|-----------|-------------|
| `query_postgres` | `sql`, `database?`, `output_format?` | Run a read-only SQL query against PostgreSQL |
| `query_clickhouse` | `sql`, `database?`, `output_format?` | Run a read-only SQL query against ClickHouse |
| `query_mysql` | `sql`, `database?`, `output_format?` | Run a read-only SQL query against MySQL |
| `query_mariadb` | `sql`, `database?`, `output_format?` | Run a read-only SQL query against MariaDB |
| `list_databases` | (none) | List all configured database connections |
| `list_tables` | `database`, `schema?` | List all tables in a database (optionally scoped to a specific schema) |
| `describe_table` | `database`, `table` | Columns, types, nullability (+ engine/rows/keys for CH and MySQL/MariaDB) |
| `sample_table` | `database`, `table`, `n?`, `output_format?` | First N rows of a table (validated `SELECT * LIMIT n`) |
| `explain_query` | `sql`, `database`, `analyze?` | Show the query execution plan |
| `usage_guide` | (none) | Returns a cheatsheet — call once to learn the full tool surface |

## Database user setup

The MCP server assumes read-only database users already exist. This is a one-time setup task for the database operator.

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
```

**Do not** grant `EXECUTE`, `TRIGGER`, `CREATE ROUTINE`, or any DML. Granting `EXECUTE` on stored functions is especially risky because `SELECT my_write_function()` parses as a pure SELECT but can perform writes inside the function body — this is the classic bypass that Layer 3 (DB privileges) exists to prevent.

(`FLUSH PRIVILEGES` is **not** needed here. It's only required when modifying the `mysql.user` table directly; `CREATE USER` / `GRANT` update the in-memory privilege cache automatically.)

### MariaDB

```sql
CREATE USER 'ai_reader'@'%' IDENTIFIED BY 'strong_password_here';
GRANT SELECT ON legacy_app.* TO 'ai_reader'@'%';
```

Same privilege posture as MySQL.

## Known limitations

### No rate limiting or query cost estimation

The server does not currently limit how many queries an AI agent can run or estimate query cost before execution. The only protection is the per-query timeout (`QUERY_TIMEOUT_SECONDS`). An AI agent could potentially:
- Run many expensive queries in parallel (up to the connection pool size)
- Execute queries that consume significant database resources (large joins, cartesian products, etc.)
- Generate query load that impacts production workloads

**Mitigations:**
- Set conservative `QUERY_TIMEOUT_SECONDS` and `MAX_RESULT_ROWS` limits
- Use a dedicated read replica for AI queries (not the primary database)
- Monitor database load and set resource limits at the database level:
  - PostgreSQL: `statement_timeout`, `statement_mem`, `work_mem`
  - ClickHouse: `max_memory_usage`, `max_execution_time`
  - MySQL: `max_execution_time` (SELECT-only, ms), `max_allowed_packet`
  - MariaDB: `max_statement_time` (all statements, seconds)
- Run the MCP server in an environment where you can control/rate-limit the AI agent's access

### Stored procedures can bypass SQL validation

See [Important: Layer 3 is not optional](#important-layer-3-is-not-optional) above.

## License

MIT
