"""Tests for result formatting module."""

import json

from readonly_db_mcp.formatting import format_markdown_table, format_results


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
        # No columns AND no rows — nothing useful to render.
        assert result == "Query returned 0 rows."

    def test_no_rows_with_columns_shows_empty_table(self) -> None:
        # Previously returned just "Query returned 0 rows." — losing the schema
        # context. Now we render headers + a 0-rows note so the AI can see
        # what columns *would* have come back.
        result = format_markdown_table(columns=["id", "name"], rows=[], total_count=0)
        assert "| id | name |" in result
        assert "| --- | --- |" in result
        assert "Query returned 0 rows" in result

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

    def test_long_cell_value_on_single_row_switches_to_vertical(self) -> None:
        # For single-row results, a cell over the truncation limit triggers
        # an auto-switch to vertical layout so the AI gets the full value
        # instead of "...". Truncation still applies for multi-row results
        # (see test_long_cell_value_truncated_in_multi_row).
        long_value = "x" * 300
        result = format_markdown_table(
            columns=["data"],
            rows=[(long_value,)],
            total_count=1,
        )
        # Vertical layout marker
        assert "-[ RECORD 1 ]-" in result
        # Full value is present, not truncated
        assert long_value in result

    def test_long_cell_value_truncated_in_multi_row(self) -> None:
        long_value = "x" * 300
        result = format_markdown_table(
            columns=["data"],
            rows=[(long_value,), ("short",)],
            total_count=2,
        )
        assert "..." in result
        # Full long value not present (truncated at 197)
        assert long_value not in result
        # Truncation note points the AI at the remedy
        assert "output_format" in result

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


class TestVerticalFormat:
    def test_basic_vertical(self) -> None:
        result = format_results(
            columns=["id", "name"],
            rows=[(1, "Alice"), (2, "Bob")],
            total_count=2,
            output_format="vertical",
        )
        assert "-[ RECORD 1 ]-" in result
        assert "-[ RECORD 2 ]-" in result
        assert "id   = 1" in result
        assert "name = Alice" in result

    def test_vertical_no_truncation(self) -> None:
        long_value = "y" * 500
        result = format_results(
            columns=["data"],
            rows=[(long_value,)],
            total_count=1,
            output_format="vertical",
        )
        assert long_value in result

    def test_vertical_null(self) -> None:
        result = format_results(
            columns=["id", "email"],
            rows=[(1, None)],
            total_count=1,
            output_format="vertical",
        )
        assert "email = NULL" in result

    def test_vertical_truncation_note(self) -> None:
        result = format_results(
            columns=["id"],
            rows=[(1,), (2,)],
            total_count=100,
            output_format="vertical",
        )
        assert "results truncated" in result


class TestJsonFormat:
    """Contract: output_format="json" always returns strictly parseable JSON."""

    def test_basic_json_envelope(self) -> None:
        result = format_results(
            columns=["id", "name"],
            rows=[(1, "Alice"), (2, "Bob")],
            total_count=2,
            output_format="json",
        )
        data = json.loads(result)
        assert data == {
            "columns": ["id", "name"],
            "rows": [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}],
            "shown": 2,
            "truncated": False,
        }

    def test_json_null(self) -> None:
        result = format_results(
            columns=["id", "email"],
            rows=[(1, None)],
            total_count=1,
            output_format="json",
        )
        data = json.loads(result)
        assert data["rows"] == [{"id": 1, "email": None}]

    def test_json_no_cell_truncation(self) -> None:
        long_value = "z" * 500
        result = format_results(
            columns=["data"],
            rows=[(long_value,)],
            total_count=1,
            output_format="json",
        )
        data = json.loads(result)
        assert data["rows"][0]["data"] == long_value

    def test_json_truncation_marks_envelope_but_stays_valid_json(self) -> None:
        # Regression: previously appended a markdown note AFTER the JSON array,
        # which broke json.loads() on the raw result. Now the envelope carries
        # truncation state as a field, and the whole payload is strictly JSON.
        result = format_results(
            columns=["id"],
            rows=[(1,), (2,)],
            total_count=100,
            output_format="json",
        )
        # The ENTIRE result is valid JSON (no markdown note appended)
        data = json.loads(result)
        assert data["rows"] == [{"id": 1}, {"id": 2}]
        assert data["shown"] == 2
        assert data["truncated"] is True

    def test_json_coerces_bytes_to_string(self) -> None:
        result = format_results(
            columns=["data"],
            rows=[(b"hello",)],
            total_count=1,
            output_format="json",
        )
        data = json.loads(result)
        assert data["rows"] == [{"data": "hello"}]
