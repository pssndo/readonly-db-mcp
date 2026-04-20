"""Tests for configuration parsing."""

import pytest

from readonly_db_mcp.config import load_config


class TestLoadConfig:
    """Tests for environment variable parsing."""

    def test_single_postgres(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("PG_1_NAME", "mydb")
        monkeypatch.setenv("PG_1_HOST", "localhost")
        monkeypatch.setenv("PG_1_PORT", "5432")
        monkeypatch.setenv("PG_1_DATABASE", "testdb")
        monkeypatch.setenv("PG_1_USER", "reader")
        monkeypatch.setenv("PG_1_PASSWORD", "secret")

        config = load_config()
        assert len(config.postgres_connections) == 1
        assert config.postgres_connections[0].name == "mydb"
        assert config.postgres_connections[0].host == "localhost"
        assert config.postgres_connections[0].port == 5432
        assert config.postgres_connections[0].database == "testdb"
        assert config.postgres_connections[0].user == "reader"
        assert config.postgres_connections[0].password == "secret"
        assert len(config.clickhouse_connections) == 0

    def test_single_clickhouse(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("CH_1_NAME", "analytics")
        monkeypatch.setenv("CH_1_HOST", "ch-host")
        monkeypatch.setenv("CH_1_PORT", "8123")
        monkeypatch.setenv("CH_1_DATABASE", "events")
        monkeypatch.setenv("CH_1_USER", "reader")
        monkeypatch.setenv("CH_1_PASSWORD", "secret")

        config = load_config()
        assert len(config.clickhouse_connections) == 1
        assert config.clickhouse_connections[0].name == "analytics"
        assert config.clickhouse_connections[0].port == 8123
        # secure defaults to False when CH_N_SECURE is not set
        assert config.clickhouse_connections[0].secure is False
        assert len(config.postgres_connections) == 0

    def test_clickhouse_secure_true(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """CH_1_SECURE=true enables TLS — required for ClickHouse Cloud."""
        monkeypatch.setenv("CH_1_NAME", "cloud")
        monkeypatch.setenv("CH_1_HOST", "abc.europe-west4.gcp.clickhouse.cloud")
        monkeypatch.setenv("CH_1_PORT", "8443")
        monkeypatch.setenv("CH_1_DATABASE", "default")
        monkeypatch.setenv("CH_1_USER", "reader")
        monkeypatch.setenv("CH_1_PASSWORD", "secret")
        monkeypatch.setenv("CH_1_SECURE", "true")

        config = load_config()
        assert config.clickhouse_connections[0].secure is True

    def test_clickhouse_secure_accepts_variants(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """CH_N_SECURE accepts true/1/yes/on (case-insensitive)."""
        for value in ("true", "TRUE", "1", "yes", "YES", "on"):
            monkeypatch.setenv("CH_1_NAME", "ch")
            monkeypatch.setenv("CH_1_HOST", "h")
            monkeypatch.setenv("CH_1_DATABASE", "d")
            monkeypatch.setenv("CH_1_USER", "u")
            monkeypatch.setenv("CH_1_PASSWORD", "p")
            monkeypatch.setenv("CH_1_SECURE", value)
            config = load_config()
            assert config.clickhouse_connections[0].secure is True, f"Expected True for {value!r}"

    def test_clickhouse_secure_falsy_variants(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """CH_N_SECURE treats false/0/no/off/empty as False."""
        for value in ("false", "FALSE", "0", "no", "off", ""):
            monkeypatch.setenv("CH_1_NAME", "ch")
            monkeypatch.setenv("CH_1_HOST", "h")
            monkeypatch.setenv("CH_1_DATABASE", "d")
            monkeypatch.setenv("CH_1_USER", "u")
            monkeypatch.setenv("CH_1_PASSWORD", "p")
            monkeypatch.setenv("CH_1_SECURE", value)
            config = load_config()
            assert config.clickhouse_connections[0].secure is False, f"Expected False for {value!r}"

    def test_multiple_postgres(self, monkeypatch: pytest.MonkeyPatch) -> None:
        for i in (1, 2):
            monkeypatch.setenv(f"PG_{i}_NAME", f"db{i}")
            monkeypatch.setenv(f"PG_{i}_HOST", f"host{i}")
            monkeypatch.setenv(f"PG_{i}_PORT", "5432")
            monkeypatch.setenv(f"PG_{i}_DATABASE", f"database{i}")
            monkeypatch.setenv(f"PG_{i}_USER", "user")
            monkeypatch.setenv(f"PG_{i}_PASSWORD", "pass")

        config = load_config()
        assert len(config.postgres_connections) == 2
        assert config.postgres_connections[0].name == "db1"
        assert config.postgres_connections[1].name == "db2"

    def test_mixed_pg_and_ch(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("PG_1_NAME", "pg1")
        monkeypatch.setenv("PG_1_HOST", "pghost")
        monkeypatch.setenv("PG_1_DATABASE", "pgdb")
        monkeypatch.setenv("PG_1_USER", "u")
        monkeypatch.setenv("PG_1_PASSWORD", "p")

        monkeypatch.setenv("CH_1_NAME", "ch1")
        monkeypatch.setenv("CH_1_HOST", "chhost")
        monkeypatch.setenv("CH_1_DATABASE", "chdb")
        monkeypatch.setenv("CH_1_USER", "u")
        monkeypatch.setenv("CH_1_PASSWORD", "p")

        config = load_config()
        assert len(config.postgres_connections) == 1
        assert len(config.clickhouse_connections) == 1

    def test_no_connections_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Explicitly remove any PG/CH env vars that might exist in the test
        # environment (e.g. CI machines with real DB connections configured).
        # monkeypatch.delenv with raising=False is a no-op if the var doesn't exist.
        # Use a wide range (1-100) since the loader scans all env vars by regex
        # and doesn't stop at gaps — PG_50_NAME would still be found.
        for prefix in ("PG_", "CH_"):
            for i in range(1, 101):
                for suffix in ("NAME", "HOST", "PORT", "DATABASE", "USER", "PASSWORD", "SECURE"):
                    monkeypatch.delenv(f"{prefix}{i}_{suffix}", raising=False)
        with pytest.raises(ValueError, match="No database connections configured"):
            load_config()

    def test_default_port_postgres(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("PG_1_NAME", "mydb")
        monkeypatch.setenv("PG_1_HOST", "localhost")
        monkeypatch.setenv("PG_1_DATABASE", "testdb")
        monkeypatch.setenv("PG_1_USER", "reader")
        monkeypatch.setenv("PG_1_PASSWORD", "secret")
        # PG_1_PORT not set — should default to 5432

        config = load_config()
        assert config.postgres_connections[0].port == 5432

    def test_default_port_clickhouse(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("CH_1_NAME", "analytics")
        monkeypatch.setenv("CH_1_HOST", "ch-host")
        monkeypatch.setenv("CH_1_DATABASE", "events")
        monkeypatch.setenv("CH_1_USER", "reader")
        monkeypatch.setenv("CH_1_PASSWORD", "secret")
        # CH_1_PORT not set — should default to 8123

        config = load_config()
        assert config.clickhouse_connections[0].port == 8123

    def test_custom_global_settings(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("PG_1_NAME", "mydb")
        monkeypatch.setenv("PG_1_HOST", "localhost")
        monkeypatch.setenv("PG_1_DATABASE", "testdb")
        monkeypatch.setenv("PG_1_USER", "reader")
        monkeypatch.setenv("PG_1_PASSWORD", "secret")
        monkeypatch.setenv("QUERY_TIMEOUT_SECONDS", "60")
        monkeypatch.setenv("MAX_RESULT_ROWS", "500")

        config = load_config()
        assert config.query_timeout_seconds == 60
        assert config.max_result_rows == 500

    def test_default_global_settings(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("PG_1_NAME", "mydb")
        monkeypatch.setenv("PG_1_HOST", "localhost")
        monkeypatch.setenv("PG_1_DATABASE", "testdb")
        monkeypatch.setenv("PG_1_USER", "reader")
        monkeypatch.setenv("PG_1_PASSWORD", "secret")

        config = load_config()
        assert config.query_timeout_seconds == 30
        assert config.max_result_rows == 1000

    def test_gap_in_numbering_finds_all(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """PG_1_ exists, PG_2_ missing, PG_3_ exists — should find both."""
        monkeypatch.setenv("PG_1_NAME", "db1")
        monkeypatch.setenv("PG_1_HOST", "host1")
        monkeypatch.setenv("PG_1_DATABASE", "d1")
        monkeypatch.setenv("PG_1_USER", "u")
        monkeypatch.setenv("PG_1_PASSWORD", "p")

        # Skip PG_2_, set PG_3_
        monkeypatch.setenv("PG_3_NAME", "db3")
        monkeypatch.setenv("PG_3_HOST", "host3")
        monkeypatch.setenv("PG_3_DATABASE", "d3")
        monkeypatch.setenv("PG_3_USER", "u")
        monkeypatch.setenv("PG_3_PASSWORD", "p")

        config = load_config()
        assert len(config.postgres_connections) == 2
        assert config.postgres_connections[0].name == "db1"
        assert config.postgres_connections[1].name == "db3"

    def test_partial_pg_config_raises_with_missing_key(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """PG_1_NAME set but PG_1_HOST missing — should raise with clear message."""
        monkeypatch.setenv("PG_1_NAME", "mydb")
        # Missing PG_1_HOST, PG_1_DATABASE, PG_1_USER, PG_1_PASSWORD
        with pytest.raises(ValueError, match="Missing required environment variable PG_1_HOST"):
            load_config()

    def test_partial_pg_config_missing_name_is_detected(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """PG_1_HOST set but PG_1_NAME missing — should raise missing NAME error."""
        monkeypatch.setenv("PG_1_HOST", "localhost")
        monkeypatch.setenv("PG_1_DATABASE", "testdb")
        monkeypatch.setenv("PG_1_USER", "reader")
        monkeypatch.setenv("PG_1_PASSWORD", "secret")
        with pytest.raises(ValueError, match="Missing required environment variable PG_1_NAME"):
            load_config()

    def test_partial_ch_config_raises_with_missing_key(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """CH_1_NAME set but CH_1_HOST missing — should raise with clear message."""
        monkeypatch.setenv("CH_1_NAME", "analytics")
        # Missing CH_1_HOST, etc.
        with pytest.raises(ValueError, match="Missing required environment variable CH_1_HOST"):
            load_config()

    def test_partial_ch_config_missing_name_is_detected(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """CH_1_HOST set but CH_1_NAME missing — should raise missing NAME error."""
        monkeypatch.setenv("CH_1_HOST", "ch-host")
        monkeypatch.setenv("CH_1_DATABASE", "events")
        monkeypatch.setenv("CH_1_USER", "reader")
        monkeypatch.setenv("CH_1_PASSWORD", "secret")
        with pytest.raises(ValueError, match="Missing required environment variable CH_1_NAME"):
            load_config()

    def test_zero_timeout_rejected(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("PG_1_NAME", "mydb")
        monkeypatch.setenv("PG_1_HOST", "localhost")
        monkeypatch.setenv("PG_1_DATABASE", "testdb")
        monkeypatch.setenv("PG_1_USER", "reader")
        monkeypatch.setenv("PG_1_PASSWORD", "secret")
        monkeypatch.setenv("QUERY_TIMEOUT_SECONDS", "0")
        with pytest.raises(ValueError, match="QUERY_TIMEOUT_SECONDS must be >= 1"):
            load_config()

    def test_negative_timeout_rejected(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("PG_1_NAME", "mydb")
        monkeypatch.setenv("PG_1_HOST", "localhost")
        monkeypatch.setenv("PG_1_DATABASE", "testdb")
        monkeypatch.setenv("PG_1_USER", "reader")
        monkeypatch.setenv("PG_1_PASSWORD", "secret")
        monkeypatch.setenv("QUERY_TIMEOUT_SECONDS", "-5")
        with pytest.raises(ValueError, match="QUERY_TIMEOUT_SECONDS must be >= 1"):
            load_config()

    def test_zero_max_rows_rejected(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("PG_1_NAME", "mydb")
        monkeypatch.setenv("PG_1_HOST", "localhost")
        monkeypatch.setenv("PG_1_DATABASE", "testdb")
        monkeypatch.setenv("PG_1_USER", "reader")
        monkeypatch.setenv("PG_1_PASSWORD", "secret")
        monkeypatch.setenv("MAX_RESULT_ROWS", "0")
        with pytest.raises(ValueError, match="MAX_RESULT_ROWS must be >= 1"):
            load_config()
