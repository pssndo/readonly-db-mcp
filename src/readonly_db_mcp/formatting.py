"""
Result formatting module — convert query results to markdown tables.

The AI agent reads markdown natively, so we format all query results as
markdown tables. This module handles:
    - Building the header row and separator
    - Converting each cell value to a string
    - Replacing None/NULL values with the text "NULL"
    - Escaping pipe characters (|) which would break markdown table syntax
    - Truncating very long cell values (>200 chars) to keep output readable
    - Appending a note when rows were truncated due to MAX_RESULT_ROWS

Example output:
    | id | name  |
    | --- | --- |
    | 1 | Alice |
    | 2 | Bob |
"""


def format_markdown_table(columns: list[str], rows: list[tuple], total_count: int) -> str:
    """
    Format query results as a markdown table string.

    Args:
        columns:     List of column names (e.g. ["id", "name", "email"]).
        rows:        List of row tuples. Each tuple has one value per column.
                     Values can be any type (int, str, None, bool, etc.)
                     and will be converted to strings.
        total_count: Row count reported by the backend for truncation detection.
                     If this is larger than len(rows), a truncation note is
                     appended. This value may be a lower bound when backends
                     intentionally cap fetch size (max_rows + 1).

    Returns:
        A string containing a markdown-formatted table, ready to send back
        to the AI agent.
    """
    # Handle empty results early
    if not columns:
        return "Query returned no columns."
    if not rows:
        return "Query returned 0 rows."

    # ── Build the header ─────────────────────────────────────────────────
    # Markdown tables look like:
    #   | col1 | col2 |
    #   | --- | --- |       <-- this separator row is required by markdown
    #   | val1 | val2 |
    header = "| " + " | ".join(str(c) for c in columns) + " |"
    separator = "| " + " | ".join("---" for _ in columns) + " |"

    # ── Build each data row ──────────────────────────────────────────────
    row_lines: list[str] = []
    for row in rows:
        cells: list[str] = []
        for val in row:
            if val is None:
                # Database NULLs are shown as the text "NULL" so the AI
                # can distinguish between an empty string and a real NULL
                cells.append("NULL")
            else:
                s = str(val)
                # Newlines would physically break the markdown table row,
                # splitting it across multiple lines. Replace with escaped
                # representations so the AI sees the value on one line.
                s = s.replace("\r\n", "\\n").replace("\r", "\\n").replace("\n", "\\n")
                # Pipe characters (|) are the column delimiter in markdown
                # tables, so they must be escaped with a backslash
                s = s.replace("|", "\\|")
                # Very long values (e.g. JSON blobs, base64 strings) would
                # make the table unreadable, so we truncate at 200 chars
                if len(s) > 200:
                    s = s[:197] + "..."
                cells.append(s)
        row_lines.append("| " + " | ".join(cells) + " |")

    # Join all parts: header, separator, then all data rows
    result = "\n".join([header, separator] + row_lines)

    # If the query returned more rows than we're showing (due to
    # MAX_RESULT_ROWS), append a note so the AI knows data was truncated.
    # We intentionally don't show an exact omitted-row count because backends
    # cap fetches to max_rows + 1, so exact totals are not always known.
    if total_count > len(rows):
        result += f"\n\n*Showing first {len(rows)} rows (results truncated).*"

    return result
