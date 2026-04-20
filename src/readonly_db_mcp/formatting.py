"""
Result formatting module — convert query results to the AI's preferred representation.

Three output formats are supported (selectable per-query):
    - "table"    (default): markdown table, compact, best for many rows with short values
    - "vertical" (\\gx-style): each row expanded into column=value pairs, no truncation,
                 ideal for wide cells (DDL strings, JSON blobs) or single-row results
    - "json":    machine-readable JSON envelope
                 {"columns": [...], "rows": [{column: value}, ...], "shown": N,
                 "truncated": bool}, no truncation, for when the AI needs to
                 programmatically process the result

Heuristic: when format="table" is requested but a cell exceeds the truncation limit,
the formatter automatically falls back to "vertical" for single-row results so the AI
doesn't lose data it explicitly asked for.

Rendering rules (shared across formats):
    - None/NULL values are rendered as the literal text "NULL" (AI can distinguish
      from an empty string)
    - In "table" format, pipes and newlines are escaped; values >200 chars are
      truncated with "..." (unless we auto-switch to vertical)
    - In "vertical" and "json" formats, values are rendered in full

Example outputs:
    table:
        | id | name  |
        | --- | --- |
        | 1 | Alice |
        | 2 | Bob |

    vertical:
        -[ RECORD 1 ]-
        id   = 1
        name = Alice
        -[ RECORD 2 ]-
        id   = 2
        name = Bob

    json:
        {"columns": ["id", "name"],
         "rows": [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}],
         "shown": 2, "truncated": false}
"""

import json
from typing import Literal

# Maximum length of a cell value rendered in a markdown table before we consider
# switching to vertical layout or truncating. Picked to keep tables readable on
# standard terminal widths.
_TABLE_CELL_TRUNCATE = 200

OutputFormat = Literal["table", "vertical", "json"]


def format_results(
    columns: list[str],
    rows: list[tuple],
    total_count: int,
    output_format: OutputFormat = "table",
) -> str:
    """Render query results in the requested format.

    Args:
        columns:       List of column names.
        rows:          List of row tuples (one value per column).
        total_count:   Row count from the backend (used to detect truncation).
                       If larger than len(rows), output indicates truncation
                       (note in table/vertical, flag in json envelope).
        output_format: "table" | "vertical" | "json". See module docstring.

    Returns:
        A string ready to send back to the AI agent.
    """
    if not columns:
        return "Query returned 0 rows."
    if not rows:
        # Show an empty table with just headers so the AI can see the schema
        # even when there's no data — this was previously reported as
        # "Query returned no columns" which was misleading.
        header = "| " + " | ".join(str(c) for c in columns) + " |"
        separator = "| " + " | ".join("---" for _ in columns) + " |"
        return f"{header}\n{separator}\n\n*Query returned 0 rows.*"

    if output_format == "json":
        return _format_json(columns, rows, total_count)
    if output_format == "vertical":
        return _format_vertical(columns, rows, total_count)

    # Default: markdown table. Check if any cell would be truncated — if so,
    # and we're returning a single row, auto-switch to vertical so the AI
    # gets the full value instead of "...".
    if len(rows) == 1 and _has_wide_cell(rows[0]):
        return _format_vertical(columns, rows, total_count)
    return _format_markdown_table(columns, rows, total_count)


# Backwards-compatible alias — older callers use this name.
def format_markdown_table(columns: list[str], rows: list[tuple], total_count: int) -> str:
    """Deprecated alias. Prefer format_results(..., output_format="table")."""
    return format_results(columns, rows, total_count, output_format="table")


def _has_wide_cell(row: tuple) -> bool:
    """True if any cell in the row would be truncated by the markdown renderer."""
    for val in row:
        if val is not None and len(str(val)) > _TABLE_CELL_TRUNCATE:
            return True
    return False


def _format_markdown_table(columns: list[str], rows: list[tuple], total_count: int) -> str:
    """Render as a markdown table (truncates long cells to keep the table readable)."""
    header = "| " + " | ".join(str(c) for c in columns) + " |"
    separator = "| " + " | ".join("---" for _ in columns) + " |"

    row_lines: list[str] = []
    any_truncated = False
    for row in rows:
        cells: list[str] = []
        for val in row:
            if val is None:
                cells.append("NULL")
                continue
            s = str(val)
            s = s.replace("\r\n", "\\n").replace("\r", "\\n").replace("\n", "\\n")
            s = s.replace("|", "\\|")
            if len(s) > _TABLE_CELL_TRUNCATE:
                s = s[: _TABLE_CELL_TRUNCATE - 3] + "..."
                any_truncated = True
            cells.append(s)
        row_lines.append("| " + " | ".join(cells) + " |")

    result = "\n".join([header, separator] + row_lines)

    if total_count > len(rows):
        result += f"\n\n*Showing first {len(rows)} rows (results truncated).*"
    if any_truncated:
        result += "\n\n*Note: some cell values were truncated. Retry with output_format=\"vertical\" or \"json\" to see full values.*"

    return result


def _format_vertical(columns: list[str], rows: list[tuple], total_count: int) -> str:
    """Render as psql \\gx-style vertical records (no cell truncation)."""
    col_width = max(len(c) for c in columns)
    parts: list[str] = []
    for i, row in enumerate(rows, start=1):
        parts.append(f"-[ RECORD {i} ]-")
        for col, val in zip(columns, row):
            rendered = "NULL" if val is None else str(val)
            parts.append(f"{col.ljust(col_width)} = {rendered}")
    if total_count > len(rows):
        parts.append("")
        parts.append(f"*Showing first {len(rows)} rows (results truncated).*")
    return "\n".join(parts)


def _format_json(columns: list[str], rows: list[tuple], total_count: int) -> str:
    """Render as a JSON envelope — always strictly parseable by json.loads().

    The envelope has a fixed shape regardless of truncation or row count:
        {
          "columns": [col1, col2, ...],     # schema, useful even for 0 rows
          "rows":    [{col: val, ...}, ...],
          "shown":   N,                      # len(rows)
          "truncated": bool                  # true if backend capped results
        }

    Contract: the returned string is ALWAYS valid JSON. Clients can safely
    do `json.loads(result)` without splitting on newlines or checking for
    appended markdown notes. A `.get("truncated")` check tells them whether
    to prompt for more specific query criteria.
    """
    records = [
        {col: _json_safe(val) for col, val in zip(columns, row)}
        for row in rows
    ]
    envelope = {
        "columns": columns,
        "rows": records,
        "shown": len(records),
        "truncated": total_count > len(rows),
    }
    return json.dumps(envelope, ensure_ascii=False, default=str)


def _json_safe(val: object) -> object:
    """Coerce values that json.dumps can't natively serialize into strings.

    We pass default=str to json.dumps as a catch-all, but pre-coercing here
    keeps the output deterministic for common non-primitive types (Decimal,
    datetime, UUID, bytes, etc.) instead of relying on repr fallback.
    """
    if val is None or isinstance(val, (str, int, float, bool, list, dict)):
        return val
    # Everything else (Decimal, datetime, UUID, memoryview, ...) as string
    if isinstance(val, (bytes, bytearray, memoryview)):
        try:
            return bytes(val).decode("utf-8")
        except UnicodeDecodeError:
            return bytes(val).hex()
    return str(val)
