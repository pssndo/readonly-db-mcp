"""Tests for SQL read-only validation — the critical security path."""

import pytest

from readonly_db_mcp.validation import validate_read_only


# ---------------------------------------------------------------------------
# Should PASS — valid read-only queries
# ---------------------------------------------------------------------------


class TestAllowedQueries:
    """Queries that must pass validation."""

    def test_simple_select(self) -> None:
        validate_read_only("SELECT * FROM users", dialect="postgres")

    def test_select_with_join(self) -> None:
        validate_read_only(
            "SELECT a, b FROM t1 JOIN t2 ON t1.id = t2.id",
            dialect="postgres",
        )

    def test_subquery_in_where(self) -> None:
        validate_read_only(
            "SELECT * FROM t1 WHERE id IN (SELECT id FROM t2)",
            dialect="postgres",
        )

    def test_cte_select(self) -> None:
        validate_read_only(
            "WITH cte AS (SELECT * FROM t1) SELECT * FROM cte",
            dialect="postgres",
        )

    def test_group_by_having(self) -> None:
        validate_read_only(
            "SELECT count(*) FROM events GROUP BY date HAVING count(*) > 10",
            dialect="postgres",
        )

    def test_window_function(self) -> None:
        validate_read_only(
            "SELECT *, ROW_NUMBER() OVER (PARTITION BY x ORDER BY y) FROM t",
            dialect="postgres",
        )

    def test_union_all(self) -> None:
        validate_read_only(
            "SELECT * FROM t1 UNION ALL SELECT * FROM t2",
            dialect="postgres",
        )

    def test_except(self) -> None:
        validate_read_only(
            "SELECT * FROM t1 EXCEPT SELECT * FROM t2",
            dialect="postgres",
        )

    def test_intersect(self) -> None:
        validate_read_only(
            "SELECT * FROM t1 INTERSECT SELECT * FROM t2",
            dialect="postgres",
        )

    def test_clickhouse_count_no_args(self) -> None:
        validate_read_only("SELECT count() FROM events", dialect="clickhouse")

    def test_clickhouse_array_join(self) -> None:
        validate_read_only(
            "SELECT * FROM events ARRAY JOIN tags",
            dialect="clickhouse",
        )

    def test_postgres_cast(self) -> None:
        validate_read_only("SELECT id::text FROM users", dialect="postgres")

    def test_postgres_lateral(self) -> None:
        validate_read_only(
            "SELECT * FROM LATERAL (SELECT 1) sub",
            dialect="postgres",
        )

    def test_trailing_semicolon(self) -> None:
        validate_read_only("SELECT 1;", dialect="postgres")

    def test_select_with_limit(self) -> None:
        validate_read_only("SELECT * FROM users LIMIT 10", dialect="postgres")

    def test_select_with_order_by(self) -> None:
        validate_read_only(
            "SELECT * FROM users ORDER BY created_at DESC",
            dialect="postgres",
        )

    def test_select_distinct(self) -> None:
        validate_read_only("SELECT DISTINCT name FROM users", dialect="postgres")

    def test_nested_subquery(self) -> None:
        validate_read_only(
            "SELECT * FROM (SELECT * FROM (SELECT 1 AS x) a) b",
            dialect="postgres",
        )


# ---------------------------------------------------------------------------
# Should FAIL — write operations and invalid queries
# ---------------------------------------------------------------------------


class TestBlockedQueries:
    """Queries that must be rejected by validation."""

    def test_insert(self) -> None:
        with pytest.raises(ValueError, match="Only SELECT queries are allowed"):
            validate_read_only("INSERT INTO users VALUES (1, 'x')", dialect="postgres")

    def test_update(self) -> None:
        with pytest.raises(ValueError, match="Only SELECT queries are allowed"):
            validate_read_only("UPDATE users SET name = 'x'", dialect="postgres")

    def test_delete(self) -> None:
        with pytest.raises(ValueError, match="Only SELECT queries are allowed"):
            validate_read_only("DELETE FROM users WHERE id = 1", dialect="postgres")

    def test_drop_table(self) -> None:
        with pytest.raises(ValueError, match="Only SELECT queries are allowed"):
            validate_read_only("DROP TABLE users", dialect="postgres")

    def test_alter_table(self) -> None:
        with pytest.raises(ValueError, match="Only SELECT queries are allowed"):
            validate_read_only("ALTER TABLE users ADD COLUMN x INT", dialect="postgres")

    def test_truncate(self) -> None:
        with pytest.raises(ValueError, match="Only SELECT queries are allowed"):
            validate_read_only("TRUNCATE TABLE users", dialect="postgres")

    def test_create_table(self) -> None:
        with pytest.raises(ValueError, match="Only SELECT queries are allowed"):
            validate_read_only("CREATE TABLE t (id INT)", dialect="postgres")

    def test_copy(self) -> None:
        with pytest.raises(ValueError, match="Only SELECT queries are allowed"):
            validate_read_only("COPY users TO '/tmp/out'", dialect="postgres")

    def test_set_statement(self) -> None:
        with pytest.raises(ValueError, match="Only SELECT queries are allowed"):
            validate_read_only("SET transaction_read_only = off", dialect="postgres")

    def test_multi_statement_injection(self) -> None:
        with pytest.raises(ValueError, match="Multiple statements not allowed"):
            validate_read_only("SELECT 1; DROP TABLE users", dialect="postgres")

    def test_cte_with_write(self) -> None:
        with pytest.raises(ValueError, match="Forbidden operation in query"):
            validate_read_only(
                "WITH del AS (DELETE FROM users RETURNING *) SELECT * FROM del",
                dialect="postgres",
            )

    def test_empty_string(self) -> None:
        with pytest.raises(ValueError, match="Empty query"):
            validate_read_only("", dialect="postgres")

    def test_whitespace_only(self) -> None:
        with pytest.raises(ValueError, match="Empty query"):
            validate_read_only("   ", dialect="postgres")

    def test_semicolon_only(self) -> None:
        with pytest.raises(ValueError, match="Empty query"):
            validate_read_only(";", dialect="postgres")

    def test_merge(self) -> None:
        with pytest.raises(ValueError, match="Only SELECT queries are allowed"):
            validate_read_only(
                "MERGE INTO t1 USING t2 ON t1.id = t2.id WHEN MATCHED THEN UPDATE SET x = 1",
                dialect="postgres",
            )

    def test_select_into(self) -> None:
        with pytest.raises(ValueError, match="Forbidden operation in query"):
            validate_read_only("SELECT * INTO new_table FROM users", dialect="postgres")

    def test_select_into_temp(self) -> None:
        with pytest.raises(ValueError, match="Forbidden operation in query"):
            validate_read_only("SELECT * INTO TEMP new_table FROM users", dialect="postgres")

    def test_select_into_clickhouse_dialect(self) -> None:
        """SELECT INTO should also be blocked when parsed with the clickhouse dialect,
        even though ClickHouse doesn't natively support this syntax. This verifies
        the validator doesn't silently pass due to dialect-specific parsing differences."""
        with pytest.raises(ValueError):
            validate_read_only("SELECT * INTO new_table FROM users", dialect="clickhouse")


class TestErgonomicHints:
    """When a common read-intent command is rejected, point the user at the dedicated tool.
    These MUST still raise — the hint only improves the error message. It never
    accepts the query."""

    def test_show_tables_rejected_with_hint(self) -> None:
        with pytest.raises(ValueError, match="list_tables"):
            validate_read_only("SHOW TABLES", dialect="clickhouse")

    def test_show_databases_rejected_with_hint(self) -> None:
        with pytest.raises(ValueError, match="list_tables"):
            validate_read_only("SHOW DATABASES", dialect="clickhouse")

    def test_describe_rejected_with_hint(self) -> None:
        with pytest.raises(ValueError, match="describe_table"):
            validate_read_only("DESCRIBE events", dialect="clickhouse")

    def test_desc_rejected_with_hint(self) -> None:
        with pytest.raises(ValueError, match="describe_table"):
            validate_read_only("DESC events", dialect="clickhouse")

    def test_exists_rejected_with_hint(self) -> None:
        with pytest.raises(ValueError, match="list_tables|describe_table"):
            validate_read_only("EXISTS TABLE events", dialect="clickhouse")

    def test_explain_rejected_with_hint(self) -> None:
        with pytest.raises(ValueError, match="explain_query"):
            validate_read_only("EXPLAIN SELECT 1", dialect="postgres")

    def test_explain_analyze_delete_still_rejected(self) -> None:
        """Critical: EXPLAIN ANALYZE DELETE actually executes the DELETE in PostgreSQL.
        The validator MUST reject this — the hint path is only for error messaging."""
        with pytest.raises(ValueError):
            validate_read_only("EXPLAIN ANALYZE DELETE FROM users", dialect="postgres")

    def test_mysql_explain_analyze_delete_still_rejected(self) -> None:
        """Same concern for MySQL 8.0.18+ where EXPLAIN ANALYZE actually executes
        the inner query. The validator must reject this regardless of dialect."""
        with pytest.raises(ValueError):
            validate_read_only("EXPLAIN ANALYZE DELETE FROM users", dialect="mysql")


class TestMysqlDialectQueries:
    """Validator parity for MySQL/MariaDB dialect (shared dialect handles both)."""

    def test_simple_select_passes(self) -> None:
        validate_read_only("SELECT * FROM users", dialect="mysql")

    def test_backtick_identifiers(self) -> None:
        """MySQL uses backticks for quoted identifiers — must parse cleanly."""
        validate_read_only("SELECT `id`, `name` FROM `users`", dialect="mysql")

    def test_mysql_limit_offset(self) -> None:
        validate_read_only("SELECT * FROM users LIMIT 10 OFFSET 20", dialect="mysql")

    def test_mysql_insert_rejected(self) -> None:
        with pytest.raises(ValueError, match="Only SELECT queries are allowed"):
            validate_read_only("INSERT INTO users VALUES (1, 'x')", dialect="mysql")

    def test_mysql_update_rejected(self) -> None:
        with pytest.raises(ValueError, match="Only SELECT queries are allowed"):
            validate_read_only("UPDATE users SET name = 'x' WHERE id = 1", dialect="mysql")

    def test_mysql_delete_rejected(self) -> None:
        with pytest.raises(ValueError, match="Only SELECT queries are allowed"):
            validate_read_only("DELETE FROM users WHERE id = 1", dialect="mysql")

    def test_mysql_truncate_rejected(self) -> None:
        with pytest.raises(ValueError):
            validate_read_only("TRUNCATE TABLE users", dialect="mysql")

    def test_mysql_cte_with_delete_rejected(self) -> None:
        """MySQL 8.0+ supports CTEs. Writes hidden in CTEs must still be caught."""
        with pytest.raises(ValueError):
            validate_read_only(
                "WITH del AS (DELETE FROM users WHERE id = 1 RETURNING *) SELECT * FROM del",
                dialect="mysql",
            )

    def test_mysql_show_rejected_with_hint(self) -> None:
        with pytest.raises(ValueError, match="list_tables"):
            validate_read_only("SHOW TABLES", dialect="mysql")

    def test_mysql_describe_rejected_with_hint(self) -> None:
        with pytest.raises(ValueError, match="describe_table"):
            validate_read_only("DESCRIBE users", dialect="mysql")

    def test_mariadb_analyze_select_rejected(self) -> None:
        """MariaDB's `ANALYZE SELECT ...` executes the inner query.
        The validator parses `ANALYZE` as a Command (not a SELECT root),
        so it must be rejected."""
        with pytest.raises(ValueError):
            validate_read_only("ANALYZE SELECT * FROM users", dialect="mysql")


class TestSqliteDialectQueries:
    """Validator parity for the sqlite dialect."""

    def test_simple_select_passes(self) -> None:
        validate_read_only("SELECT * FROM users", dialect="sqlite")

    def test_cte_passes(self) -> None:
        validate_read_only(
            "WITH cte AS (SELECT 1 AS x) SELECT * FROM cte",
            dialect="sqlite",
        )

    def test_sqlite_insert_rejected(self) -> None:
        with pytest.raises(ValueError, match="Only SELECT queries are allowed"):
            validate_read_only("INSERT INTO users VALUES (1, 'x')", dialect="sqlite")

    def test_sqlite_update_rejected(self) -> None:
        with pytest.raises(ValueError, match="Only SELECT queries are allowed"):
            validate_read_only("UPDATE users SET name = 'x' WHERE id = 1", dialect="sqlite")

    def test_sqlite_delete_rejected(self) -> None:
        with pytest.raises(ValueError, match="Only SELECT queries are allowed"):
            validate_read_only("DELETE FROM users WHERE id = 1", dialect="sqlite")

    def test_sqlite_attach_database_rejected(self) -> None:
        """ATTACH DATABASE is SQLite-specific and opens another file. Even
        though our VFS read-only mode at the connection layer would also
        block it (ATTACH requires a writable main database), the validator
        must reject it at parse time for defense-in-depth."""
        with pytest.raises(ValueError):
            validate_read_only(
                "ATTACH DATABASE '/tmp/other.db' AS other",
                dialect="sqlite",
            )

    def test_sqlite_detach_rejected(self) -> None:
        with pytest.raises(ValueError):
            validate_read_only("DETACH DATABASE other", dialect="sqlite")

    def test_sqlite_create_table_rejected(self) -> None:
        with pytest.raises(ValueError):
            validate_read_only("CREATE TABLE t (id INT)", dialect="sqlite")

    def test_sqlite_pragma_writable_schema_rejected(self) -> None:
        """PRAGMA writable_schema=ON is the SQLite equivalent of escalating
        privileges — it lets SELECT modify internal catalog rows. sqlglot
        parses it as a Command, so the whitelist rejects it."""
        with pytest.raises(ValueError):
            validate_read_only("PRAGMA writable_schema = ON", dialect="sqlite")

    def test_sqlite_show_rejected_with_hint(self) -> None:
        with pytest.raises(ValueError, match="list_tables"):
            validate_read_only("SHOW TABLES", dialect="sqlite")

    def test_sqlite_select_load_extension_passes_validator(self) -> None:
        """SELECT load_extension(...) looks like a pure SELECT to the AST
        parser — the Layer 2 defense (enable_load_extension=False on the
        connection) is what actually blocks this at runtime, not the
        validator. This test documents the validator's behavior explicitly
        so future changes don't accidentally rely on it."""
        # This MUST pass the validator — it's a plain SELECT by AST shape.
        # The runtime block happens in the SQLite backend's _open_readonly.
        validate_read_only("SELECT load_extension('/tmp/evil.so')", dialect="sqlite")
