"""
Configuration module — parse database connections from environment variables or .env file.

How it works:
    Database connections are configured via environment variables with a numbered
    prefix pattern: PG_1_*, PG_2_* for PostgreSQL, CH_1_*, CH_2_* for ClickHouse.

    The loader scans all environment variables for matching prefixes and collects
    all unique IDs. So if PG_1_* and PG_3_* are set but PG_2_* is missing,
    both PG_1_ and PG_3_ are loaded (gaps are allowed).

    If a .env file exists in the working directory, it is loaded first (but
    won't overwrite any environment variables that are already set).

    Required env vars per connection:
        *_NAME      - A friendly name to identify this connection (e.g. "prod_db")
        *_HOST      - Database server hostname
        *_DATABASE  - Database name to connect to
        *_USER      - Username for authentication
        *_PASSWORD  - Password for authentication

    Optional env vars per connection:
        *_PORT      - Port number (defaults: 5432 for PG, 8123 for CH)

    Global settings:
        QUERY_TIMEOUT_SECONDS  - Max seconds per query (default: 30)
        MAX_RESULT_ROWS        - Max rows to return (default: 1000)
"""

import os
import re
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


@dataclass
class PostgresConnection:
    """Configuration for a single PostgreSQL connection.

    Each field maps to a PG_N_* environment variable.
    Example: PG_1_HOST -> host, PG_1_PORT -> port, etc.
    """

    name: str  # Friendly name (PG_N_NAME), used to identify this connection in tools
    host: str  # Server hostname (PG_N_HOST)
    port: int  # Server port (PG_N_PORT, default 5432)
    database: str  # Database name to connect to (PG_N_DATABASE)
    user: str  # Auth username (PG_N_USER) — should be a read-only DB user
    password: str  # Auth password (PG_N_PASSWORD)


@dataclass
class ClickHouseConnection:
    """Configuration for a single ClickHouse connection.

    Each field maps to a CH_N_* environment variable.
    Example: CH_1_HOST -> host, CH_1_PORT -> port, etc.
    """

    name: str  # Friendly name (CH_N_NAME), used to identify this connection in tools
    host: str  # Server hostname (CH_N_HOST)
    port: int  # HTTP interface port (CH_N_PORT, default 8123)
    database: str  # Database name to connect to (CH_N_DATABASE)
    user: str  # Auth username (CH_N_USER) — should be a read-only DB user
    password: str  # Auth password (CH_N_PASSWORD)


@dataclass
class Config:
    """Application configuration with all database connections and global settings."""

    postgres_connections: list[PostgresConnection]  # All configured PG connections
    clickhouse_connections: list[ClickHouseConnection]  # All configured CH connections
    query_timeout_seconds: int = 30  # Max seconds before a query is killed
    max_result_rows: int = 1000  # Max rows returned to the AI (rest are truncated)


def _require_env(key: str, context: str) -> str:
    """
    Get a required environment variable, raising a clear error if missing.

    Args:
        key:     The env var name to look up (e.g. "PG_1_HOST").
        context: A label for the error message (e.g. "PG_1") so the user
                 knows which connection group is incomplete.

    Raises:
        ValueError: With a message like "Missing required environment variable
                    PG_1_HOST (for PG_1)".
    """
    value = os.environ.get(key)
    if value is None:
        raise ValueError(f"Missing required environment variable {key} (for {context})")
    return value


def load_config() -> Config:
    """
    Scan environment variables for PG_N_* and CH_N_* patterns and build the Config.

    Loads .env file if present (does not override existing env vars).
    Scans all env vars for PG_N_* / CH_N_* connection keys (gaps allowed).

    Raises:
        ValueError: If no connections are configured at all, or if a connection
                    group is partially defined (e.g. PG_1_NAME is set but
                    PG_1_HOST is missing).
    """
    # Load .env file if it exists in the current directory.
    # override=False means existing env vars take priority over .env values.
    # This lets users set env vars in their shell or MCP config and still
    # have a .env file as a fallback for local development.
    env_path = Path(".env")
    if env_path.is_file():
        load_dotenv(env_path, override=False)

    # ── Scan for PostgreSQL connections ──────────────────────────────────
    # Scan all env vars for known PG_N_* keys to find unique IDs.
    # This allows gaps in numbering: PG_1_ and PG_3_ work even if PG_2_ is missing.
    # It also catches partial configs where NAME is missing but other keys exist.
    pg_ids = sorted(
        {
            int(m.group(1))
            for k in os.environ
            if (m := re.match(r"^PG_(\d+)_(NAME|HOST|PORT|DATABASE|USER|PASSWORD)$", k))
        }
    )
    pg_conns: list[PostgresConnection] = []
    for i in pg_ids:
        context = f"PG_{i}"  # Used in error messages if a required var is missing
        pg_conns.append(
            PostgresConnection(
                name=_require_env(f"PG_{i}_NAME", context),
                host=_require_env(f"PG_{i}_HOST", context),
                port=int(os.environ.get(f"PG_{i}_PORT", "5432")),  # Default PG port
                database=_require_env(f"PG_{i}_DATABASE", context),
                user=_require_env(f"PG_{i}_USER", context),
                password=_require_env(f"PG_{i}_PASSWORD", context),
            )
        )

    # ── Scan for ClickHouse connections ──────────────────────────────────
    # Same pattern: scan for known CH_N_* keys and collect all unique IDs.
    ch_ids = sorted(
        {
            int(m.group(1))
            for k in os.environ
            if (m := re.match(r"^CH_(\d+)_(NAME|HOST|PORT|DATABASE|USER|PASSWORD)$", k))
        }
    )
    ch_conns: list[ClickHouseConnection] = []
    for i in ch_ids:
        context = f"CH_{i}"
        ch_conns.append(
            ClickHouseConnection(
                name=_require_env(f"CH_{i}_NAME", context),
                host=_require_env(f"CH_{i}_HOST", context),
                port=int(os.environ.get(f"CH_{i}_PORT", "8123")),  # Default CH HTTP port
                database=_require_env(f"CH_{i}_DATABASE", context),
                user=_require_env(f"CH_{i}_USER", context),
                password=_require_env(f"CH_{i}_PASSWORD", context),
            )
        )

    # At least one database must be configured, otherwise the server is useless
    if not pg_conns and not ch_conns:
        raise ValueError(
            "No database connections configured. "
            "Set PG_1_NAME/PG_1_HOST/... or CH_1_NAME/CH_1_HOST/... environment variables."
        )

    # Parse global settings with basic bounds validation
    query_timeout = int(os.environ.get("QUERY_TIMEOUT_SECONDS", "30"))
    if query_timeout < 1:
        raise ValueError(f"QUERY_TIMEOUT_SECONDS must be >= 1, got {query_timeout}")

    max_rows = int(os.environ.get("MAX_RESULT_ROWS", "1000"))
    if max_rows < 1:
        raise ValueError(f"MAX_RESULT_ROWS must be >= 1, got {max_rows}")

    return Config(
        postgres_connections=pg_conns,
        clickhouse_connections=ch_conns,
        query_timeout_seconds=query_timeout,
        max_result_rows=max_rows,
    )
