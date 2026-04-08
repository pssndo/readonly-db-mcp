"""
Abstract database backend interface.

This module defines the contract that all database backends (PostgreSQL,
ClickHouse, etc.) must implement. Using an abstract base class ensures that
both backends expose the same methods with the same signatures, so the
server code can work with any backend interchangeably.

It also contains shared utilities (like identifier validation) used by all backends.

Key concepts for non-Python readers:
    - `ABC` (Abstract Base Class) means this class cannot be instantiated
      directly. You must create a subclass (like PostgresBackend) that
      implements all the abstract methods.
    - `@abstractmethod` marks a method that MUST be implemented by subclasses.
      If a subclass forgets to implement one, Python raises an error at
      instantiation time (not at runtime).
    - `async def` means the method is asynchronous — it can pause while
      waiting for I/O (like a database response) without blocking other work.
    - Type hints like `-> tuple[list[str], list[tuple], int]` describe what
      the method returns: a tuple containing (column names, row data, count).
"""

import re
from abc import ABC, abstractmethod

from sqlglot import parse, exp

# ── Identifier validation (shared by all backends) ───────────────────────────
# Matches safe SQL identifiers: starts with a letter or underscore, followed by
# letters, digits, underscores, or dots (dots allow schema.table notation).
# Anything else (spaces, semicolons, quotes, etc.) is rejected to prevent
# SQL injection in DESCRIBE TABLE and similar queries.
#
# Rules:
#   - Must start with a letter or underscore
#   - May contain letters, digits, underscores, and dots
#   - Empty strings are rejected
#   - Leading digits rejected (e.g. "123abc")
#
# Strictness tradeoff: This intentionally rejects identifiers with hyphens,
# spaces, or other special characters (e.g. "my-company-data"). Some databases
# allow these in quoted identifiers ("my-table"), but supporting them here would
# require quoting logic and expand the attack surface. This is the PRIMARY
# defense for ClickHouse (which uses string interpolation in DESCRIBE TABLE),
# so we err on the side of security. For Postgres, this is defense-in-depth
# since parameterized queries ($1/$2) already prevent injection.
# If quoted identifiers are needed in the future, use the database driver's
# native quoting function rather than relaxing this regex.
_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_.]*$")


def validate_identifier(name: str) -> str:
    """
    Validate that a string is a safe SQL identifier (no injection risk).

    Used to sanitize table and schema names before they are interpolated
    into SQL strings. This is critical for ClickHouse's DESCRIBE TABLE which
    uses string interpolation, and is defense-in-depth for PostgreSQL which
    uses parameterized queries.

    Args:
        name: The identifier to validate (e.g. "public.users", "events").

    Returns:
        The identifier unchanged if valid.

    Raises:
        ValueError: If the identifier contains unsafe characters.
    """
    if not _SAFE_IDENTIFIER.match(name):
        raise ValueError(f"Invalid identifier: {name!r}")
    return name


def inject_limit(sql: str, max_rows: int, dialect: str) -> str:
    """
    Inject or tighten a LIMIT clause on a SQL query using AST manipulation.

    Instead of wrapping the user's query in a subquery (which would lose ORDER BY
    guarantees in PostgreSQL), we parse the SQL, inspect/modify the LIMIT node
    directly, and regenerate the SQL string.

    If the query already has a LIMIT:
        - If existing LIMIT <= max_rows, keep it as-is (user wants fewer rows).
        - If existing LIMIT > max_rows, replace it with max_rows.
    If the query has no LIMIT:
        - Add LIMIT max_rows.

    This approach preserves ORDER BY, column names, and all other query semantics.

    Args:
        sql:      The SQL query string (already validated as read-only).
        max_rows: The maximum number of rows to allow.
        dialect:  The SQL dialect ("postgres" or "clickhouse") for correct
                  parsing and generation.

    Returns:
        The SQL string with a LIMIT clause injected or tightened.
    """
    try:
        statements = parse(sql, dialect=dialect)
        if not statements or len(statements) != 1:
            # Fallback: if parsing fails or gives unexpected results, use
            # subquery wrapping as a safe default (ORDER BY loss is better
            # than no LIMIT at all).
            return f"SELECT * FROM ({sql}) AS _limited_query LIMIT {max_rows}"

        ast = statements[0]

        # Find the existing LIMIT node (if any) on the outermost query.
        # For UNION/INTERSECT/EXCEPT, sqlglot stores the limit on the
        # compound expression itself.
        existing_limit = ast.args.get("limit")

        if existing_limit:
            # Extract the numeric value from the existing LIMIT expression.
            # The LIMIT node wraps an expression (usually a Literal).
            limit_expr = existing_limit.expression
            if isinstance(limit_expr, exp.Literal) and limit_expr.is_int:
                current_limit = int(limit_expr.this)
                if current_limit > max_rows:
                    # User's LIMIT is too high — tighten it
                    ast.args["limit"] = exp.Limit(expression=exp.Literal.number(max_rows))
                # else: user's LIMIT is already within bounds, keep it
            else:
                # LIMIT is a non-literal expression (e.g. a parameter or subquery).
                # Replace with our hard cap to be safe.
                ast.args["limit"] = exp.Limit(expression=exp.Literal.number(max_rows))
        else:
            # No LIMIT present — add one
            ast.args["limit"] = exp.Limit(expression=exp.Literal.number(max_rows))

        return ast.sql(dialect=dialect)
    except Exception:
        # If AST manipulation fails for any reason, fall back to subquery
        # wrapping. Losing ORDER BY is acceptable as a fallback — it's better
        # than running an unlimited query.
        return f"SELECT * FROM ({sql}) AS _limited_query LIMIT {max_rows}"


class DatabaseBackend(ABC):
    """
    Base class for all database backends.

    Every backend must set these class attributes:
        db_type:  A string identifying the database type ("postgres" or "clickhouse").
                  Used by the server to route queries to the right backend.
        host:     The hostname of the database server (for display in list_databases).
        database: The database name (for display in list_databases).
    """

    db_type: str  # "postgres" or "clickhouse"
    host: str  # Database server hostname
    database: str  # Database name

    @abstractmethod
    async def connect(self) -> None:
        """
        Establish the database connection or connection pool.
        Called once during server startup. Must be called before any other method.
        """
        ...

    @abstractmethod
    async def disconnect(self) -> None:
        """
        Close the database connection or pool and release all resources.
        Called once during server shutdown.
        """
        ...

    @abstractmethod
    async def execute(self, sql: str) -> tuple[list[str], list[tuple], int]:
        """
        Execute a read-only SQL query.

        Args:
            sql: The SQL query string (already validated as read-only).

        Returns:
            A 3-tuple of:
                - column_names: List of column names (e.g. ["id", "name"])
                - rows: List of tuples, each containing one row's values
                - total_count: Total number of rows before truncation. If this
                  is larger than len(rows), the result was truncated to
                  MAX_RESULT_ROWS.
        """
        ...

    @abstractmethod
    async def list_tables(self) -> list[str]:
        """
        List all user-accessible tables in the database.

        Returns:
            List of table names. For PostgreSQL, these are in "schema.table"
            format (e.g. "public.users"). For ClickHouse, just table names.
        """
        ...

    @abstractmethod
    async def describe_table(self, table: str) -> list[dict]:
        """
        Describe the columns, data types, and nullability for a table.

        Args:
            table: Table name. For PostgreSQL, can be "schema.table" or just
                   "table" (defaults to the "public" schema).

        Returns:
            List of dicts, each with keys: "name", "type", "nullable".
            Example: [{"name": "id", "type": "integer", "nullable": "NO"}]
        """
        ...

    @abstractmethod
    async def explain(self, sql: str, analyze: bool = False) -> str:
        """
        Return the query execution plan as text.

        Args:
            sql:     The SQL query to explain.
            analyze: If True, actually run the query and show real timing data
                     (PostgreSQL only). ClickHouse does not support EXPLAIN
                     ANALYZE — the flag is accepted but ignored, and a note
                     is appended to the output.

        Returns:
            The execution plan as a multi-line string.
        """
        ...
