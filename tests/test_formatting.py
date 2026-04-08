"""Tests for result formatting module."""

from readonly_db_mcp.formatting import format_markdown_table


class TestFormatMarkdownTable:
    """Tests for markdown table formatting."""

    def test_basic_table(self) -> None:
        result = format_markdown_table(
            columns=["id", "name"],
            rows=[(1, "Alice"), (2, "Bob")],
            total_count=2,
        )
        assert "| id | name |" in result
        assert "| --- | --- |" in result
        assert "| 1 | Alice |" in result
        assert "| 2 | Bob |" in result

    def test_no_columns(self) -> None:
        result = format_markdown_table(columns=[], rows=[], total_count=0)
        assert result == "Query returned no columns."

    def test_no_rows(self) -> None:
        result = format_markdown_table(columns=["id", "name"], rows=[], total_count=0)
        assert result == "Query returned 0 rows."

    def test_null_values(self) -> None:
        result = format_markdown_table(
            columns=["id", "email"],
            rows=[(1, None), (2, "bob@test.com")],
            total_count=2,
        )
        assert "| 1 | NULL |" in result
        assert "| 2 | bob@test.com |" in result

    def test_pipe_escaping(self) -> None:
        result = format_markdown_table(
            columns=["data"],
            rows=[("a|b|c",)],
            total_count=1,
        )
        assert "a\\|b\\|c" in result

    def test_truncated_rows_shows_note(self) -> None:
        result = format_markdown_table(
            columns=["id"],
            rows=[(1,), (2,)],
            total_count=100,
        )
        assert "*Showing first 2 rows (results truncated).*" in result

    def test_no_truncation_note_when_all_shown(self) -> None:
        result = format_markdown_table(
            columns=["id"],
            rows=[(1,), (2,)],
            total_count=2,
        )
        assert "Showing" not in result

    def test_long_cell_value_truncated(self) -> None:
        long_value = "x" * 300
        result = format_markdown_table(
            columns=["data"],
            rows=[(long_value,)],
            total_count=1,
        )
        # Should be truncated to 197 chars + "..."
        assert "..." in result
        assert "x" * 198 not in result
        assert "x" * 197 in result

    def test_single_column_single_row(self) -> None:
        result = format_markdown_table(
            columns=["count"],
            rows=[(42,)],
            total_count=1,
        )
        assert "| count |" in result
        assert "| 42 |" in result

    def test_various_types(self) -> None:
        result = format_markdown_table(
            columns=["int", "float", "bool", "str"],
            rows=[(1, 3.14, True, "hello")],
            total_count=1,
        )
        assert "| 1 | 3.14 | True | hello |" in result

    def test_newline_in_value_escaped(self) -> None:
        result = format_markdown_table(
            columns=["data"],
            rows=[("line1\nline2",)],
            total_count=1,
        )
        # Newline should be escaped, not break the table
        assert "line1\\nline2" in result
        assert "\n" not in result.split("\n")[2]  # The data row should be a single line

    def test_carriage_return_in_value_escaped(self) -> None:
        result = format_markdown_table(
            columns=["data"],
            rows=[("line1\r\nline2",)],
            total_count=1,
        )
        assert "line1\\nline2" in result

    def test_bare_cr_in_value_escaped(self) -> None:
        result = format_markdown_table(
            columns=["data"],
            rows=[("line1\rline2",)],
            total_count=1,
        )
        assert "line1\\nline2" in result
