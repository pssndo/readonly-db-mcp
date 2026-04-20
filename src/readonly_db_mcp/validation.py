"""
SQL validation module — whitelist approach to ensure only read-only queries are allowed.

How it works:
    1. The raw SQL string is parsed into an Abstract Syntax Tree (AST) using the
       `sqlglot` library. An AST is a tree-shaped data structure that represents
       the logical structure of the query (e.g. SELECT -> FROM -> WHERE).
    2. The ROOT node of the tree must be one of the ALLOWED_ROOT_TYPES (SELECT,
       UNION, INTERSECT, EXCEPT). Anything else (INSERT, CREATE, etc.) is
       rejected immediately.
    3. Even if the root is a SELECT, we walk every node in the entire tree to
       catch write operations hidden inside CTEs (WITH clauses) or subqueries.
       For example: `WITH x AS (DELETE FROM t RETURNING *) SELECT * FROM x`
       would pass the root check (it's a SELECT) but the tree walk catches
       the DELETE buried inside.

Why whitelist instead of blacklist:
    A blacklist would need to list every dangerous SQL keyword. If a new keyword
    is added to SQL or a dialect, the blacklist would miss it. A whitelist only
    allows known-safe operations, so unknown operations are rejected by default.
"""

from sqlglot import parse, exp
from sqlglot.errors import ParseError

# ── AST node types that indicate a write/mutation operation ──────────────────
# If ANY of these appear anywhere in the query tree, the query is rejected.
# This catches writes hidden in CTEs, subqueries, or other nested contexts.
FORBIDDEN_NODES = (
    exp.Insert,  # INSERT INTO ... VALUES ...
    exp.Update,  # UPDATE ... SET ...
    exp.Delete,  # DELETE FROM ...
    exp.Create,  # CREATE TABLE / CREATE INDEX / etc.
    exp.Drop,  # DROP TABLE / DROP INDEX / etc.
    exp.Alter,  # ALTER TABLE ... ADD COLUMN / etc.
    exp.TruncateTable,  # TRUNCATE TABLE (named TruncateTable in sqlglot, not Truncate)
    exp.Merge,  # MERGE INTO ... USING ... (upsert operation)
    exp.Command,  # Raw commands like VACUUM, GRANT, REVOKE, etc.
    exp.Set,  # SET variable = value (could disable read-only mode)
    exp.Copy,  # COPY ... TO/FROM (file I/O)
    exp.Transaction,  # BEGIN / START TRANSACTION
    exp.Commit,  # COMMIT
    exp.Rollback,  # ROLLBACK
    exp.Into,  # SELECT ... INTO new_table (creates a new table from SELECT results)
)

# ── AST node types that are allowed as the top-level (root) statement ────────
# Only SELECT-family statements can be the outermost query.
ALLOWED_ROOT_TYPES = (
    exp.Select,  # Plain SELECT
    exp.Union,  # SELECT ... UNION [ALL] SELECT ...
    exp.Intersect,  # SELECT ... INTERSECT SELECT ...
    exp.Except,  # SELECT ... EXCEPT SELECT ...
)


def validate_read_only(sql: str, dialect: str) -> str:
    """
    Validate that a SQL string contains only read-only operations.

    Args:
        sql:     The raw SQL query string from the user/AI agent.
        dialect: The SQL dialect to parse with ("postgres" or "clickhouse").
                 Different databases have slightly different SQL syntax.

    Returns:
        The cleaned SQL string (whitespace trimmed, trailing semicolons removed).
        Callers should use this return value for execution instead of the raw
        input, because the raw input may contain trailing semicolons that would
        break subquery wrapping (e.g. SELECT * FROM (SELECT 1;) — invalid).

    Raises:
        ValueError: With a human-readable message explaining why the query
                    was rejected. This message is returned to the AI agent.
    """
    # Strip whitespace and trailing semicolons before parsing.
    # Semicolons would cause sqlglot to treat "SELECT 1;" as two statements
    # (the SELECT and an empty statement after the semicolon).
    sql_stripped = sql.strip().rstrip(";")
    if not sql_stripped:
        raise ValueError("Empty query")

    # Parse the SQL into an AST (Abstract Syntax Tree).
    # `dialect` tells sqlglot which SQL flavor to expect (postgres vs clickhouse
    # have different syntax for things like type casts, array operations, etc.)
    try:
        statements = parse(sql_stripped, dialect=dialect)
    except ParseError as e:
        # Some dialect-specific commands (e.g. ClickHouse's `EXISTS TABLE x`)
        # don't parse cleanly. If the first word is a common read-intent
        # command, show the pointed error instead of the raw parse error —
        # the user's intent is clear even if sqlglot can't parse it.
        hint = _ergonomic_hint_for_rejected(sql_stripped)
        if hint:
            raise ValueError(hint)
        raise ValueError(f"SQL parse error: {e}")

    if not statements:
        raise ValueError("No valid SQL statements found")

    # Reject multi-statement input like "SELECT 1; DROP TABLE users".
    # Even though the semicolon was stripped above, sqlglot can still detect
    # multiple statements separated by semicolons within the string.
    if len(statements) > 1:
        raise ValueError("Multiple statements not allowed — send one query at a time")

    ast = statements[0]

    # ── Check 1: Root node must be SELECT-family ─────────────────────────
    # This is the first line of defense. If someone sends "DROP TABLE users",
    # the root AST node will be a Drop, which is not in ALLOWED_ROOT_TYPES.
    if not isinstance(ast, ALLOWED_ROOT_TYPES):
        # Detect common read-intent commands (SHOW, DESCRIBE, EXISTS, EXPLAIN)
        # that users often try via raw SQL, and point them at the dedicated
        # tools. We still reject — allowing raw EXPLAIN would be a bypass risk
        # (e.g. PostgreSQL's EXPLAIN ANALYZE actually executes the inner query,
        # so EXPLAIN ANALYZE DELETE FROM users would wipe the table).
        hint = _ergonomic_hint_for_rejected(sql_stripped)
        if hint:
            raise ValueError(hint)
        raise ValueError(f"Only SELECT queries are allowed. Got: {type(ast).__name__}")

    # ── Check 2: Walk the full tree looking for forbidden nodes ──────────
    # Even though the root is a SELECT, there could be write operations hidden
    # inside CTEs or subqueries. ast.walk() yields every node in the tree.
    for node in ast.walk():
        if isinstance(node, FORBIDDEN_NODES):
            raise ValueError(f"Forbidden operation in query: {type(node).__name__}")

    # Return the cleaned SQL (stripped of whitespace and trailing semicolons)
    # so callers can use it directly for execution without re-parsing.
    return sql_stripped


def _ergonomic_hint_for_rejected(sql: str) -> str | None:
    """Return a tool-pointing error message when the user tried a common read-intent
    command via raw SQL (SHOW, DESCRIBE, EXISTS, EXPLAIN). Returns None if no
    match — caller falls back to the generic rejection message.

    IMPORTANT: This function NEVER relaxes validation. It only improves the
    error message shown to the user when their query is rejected. All branches
    must raise (via the caller) — never return an "approved" status.
    """
    # Check the raw SQL prefix (sqlglot's exp.Command wraps these with varying
    # internal representations across dialects, and some dialect-specific
    # commands like ClickHouse's `EXISTS TABLE x` fail to parse at all —
    # checking the text is the most reliable signal).
    first_word = sql.strip().split(None, 1)[0].upper() if sql.strip() else ""

    if first_word in ("SHOW",):
        return (
            "SHOW commands are not allowed as raw SQL. Use the `list_tables` tool "
            "to list tables, or `list_databases` to see configured connections."
        )
    if first_word in ("DESCRIBE", "DESC"):
        return (
            "DESCRIBE is not allowed as raw SQL. Use the `describe_table` tool "
            "to get columns, types, and nullability."
        )
    if first_word == "EXISTS":
        return (
            "EXISTS table-check is not allowed as raw SQL. Use `list_tables` to "
            "see what exists, or `describe_table` which errors if the table is missing."
        )
    if first_word == "EXPLAIN":
        return (
            "EXPLAIN is not allowed as raw SQL (EXPLAIN ANALYZE can execute the "
            "inner query, which is a write-bypass risk). Use the `explain_query` "
            "tool instead — it validates the inner query is read-only first."
        )
    return None
