import pytest
import oxpg

DSN = "postgresql://postgres:test@localhost:5432/postgres"


class TestConnect:
    """Test connection functionality"""

    def test_connect_with_valid_dsn(self):
        """Should connect successfully with valid DSN"""
        client = oxpg.connect(dsn=DSN)
        assert client is not None
        assert "localhost" in repr(client)
        assert "5432" in repr(client)

    def test_connect_with_individual_params(self):
        """Should connect with individual parameters"""
        client = oxpg.connect(
            host="localhost",
            user="postgres",
            password="test",
            port=5432,
            db="postgres"
        )
        assert client is not None
        assert "localhost" in repr(client)

    def test_connect_with_default_port(self):
        """Should use default port 5432 if not specified"""
        client = oxpg.connect(
            host="localhost",
            user="postgres",
            password="test",
            db="postgres"
        )
        assert "5432" in repr(client)

    def test_connect_with_default_db(self):
        """Should use default database 'postgres' if not specified"""
        client = oxpg.connect(
            host="localhost",
            user="postgres",
            password="test"
        )
        assert "postgres" in repr(client)

    def test_connect_with_ipv4_address(self):
        """Should accept IPv4 addresses as host"""
        client = oxpg.connect(
            host="127.0.0.1",
            user="postgres",
            password="test",
            db="postgres"
        )
        assert "127.0.0.1" in repr(client)


class TestConnectionErrors:
    """Test error handling for connection parameters"""

    def test_connect_missing_host(self):
        """Should raise ValueError when host is missing"""
        with pytest.raises(ValueError, match="Missing required parameter: host"):
            oxpg.connect(user="postgres", password="test")

    def test_connect_missing_user(self):
        """Should raise ValueError when user is missing"""
        with pytest.raises(ValueError, match="Missing required parameter: user"):
            oxpg.connect(host="localhost", password="test")

    def test_connect_missing_password(self):
        """Should raise ValueError when password is missing"""
        with pytest.raises(ValueError, match="Missing required parameter: password"):
            oxpg.connect(host="localhost", user="postgres")

    def test_connect_dsn_and_host(self):
        """Should raise ValueError when both DSN and host provided"""
        with pytest.raises(ValueError, match="Cannot specify both DSN"):
            oxpg.connect(dsn=DSN, host="localhost")

    def test_connect_dsn_and_user(self):
        """Should raise ValueError when DSN and user provided"""
        with pytest.raises(ValueError, match="Cannot specify both DSN"):
            oxpg.connect(dsn=DSN, user="postgres")

    def test_connect_dsn_and_password(self):
        """Should raise ValueError when DSN and password provided"""
        with pytest.raises(ValueError, match="Cannot specify both DSN"):
            oxpg.connect(dsn=DSN, password="test")


class TestDSNParsing:
    """Test DSN parsing and validation"""

    def test_invalid_dsn_scheme(self):
        """Should raise ValueError for invalid scheme"""
        with pytest.raises(ValueError, match="Invalid DSN"):
            oxpg.connect(dsn="mysql://user:pass@localhost/db")

    def test_invalid_dsn_missing_user(self):
        """Should raise ValueError when user is missing from DSN"""
        with pytest.raises(ValueError, match="Missing required parameter: user"):
            oxpg.connect(dsn="postgresql://localhost/db")

    def test_invalid_dsn_missing_database(self):
        """Should raise ValueError when database name is missing from DSN"""
        with pytest.raises(ValueError, match="Missing required parameter: database"):
            oxpg.connect(dsn="postgresql://user:pass@localhost")

    def test_valid_dsn_without_port(self):
        """Should accept DSN without explicit port (defaults to 5432)"""
        client = oxpg.connect(
            dsn="postgresql://postgres:test@localhost/postgres")
        assert "5432" in repr(client)

    def test_valid_dsn_with_postgres_scheme(self):
        """Should accept 'postgres://' scheme (not just 'postgresql://')"""
        client = oxpg.connect(
            dsn="postgres://postgres:test@localhost:5432/postgres")
        assert client is not None

    def test_dsn_with_ipv4_host(self):
        """Should accept IPv4 addresses in DSN"""
        client = oxpg.connect(
            dsn="postgresql://postgres:test@127.0.0.1:5432/postgres")
        assert "127.0.0.1" in repr(client)

    def test_dsn_with_non_standard_port(self):
        """Should parse DSN with non-standard port"""
        with pytest.raises(ConnectionError):
            oxpg.connect(
                dsn="postgresql://postgres:test@localhost:9999/postgres")


class TestConnectionFailure:
    """Test actual connection failures"""

    def test_connect_to_invalid_host(self):
        """Should raise ConnectionError for invalid/unreachable host"""
        with pytest.raises(ConnectionError, match="Failed to connect to PostgreSQL"):
            oxpg.connect(
                host="nonexistent.host.invalid",
                user="postgres",
                password="test",
                db="postgres"
            )

    def test_connect_with_wrong_credentials(self):
        """Should raise ConnectionError for wrong username/password"""
        with pytest.raises(ConnectionError, match="Failed to connect to PostgreSQL"):
            oxpg.connect(
                host="localhost",
                user="wronguser",
                password="wrongpass",
                db="postgres"
            )

    def test_connect_to_wrong_port(self):
        """Should raise ConnectionError when connecting to wrong port"""
        with pytest.raises(ConnectionError, match="Failed to connect to PostgreSQL"):
            oxpg.connect(
                host="localhost",
                user="postgres",
                password="test",
                port=9999,
                db="postgres"
            )

    def test_connect_to_nonexistent_database(self):
        """Should raise ConnectionError for non-existent database"""
        with pytest.raises(ConnectionError, match="Failed to connect to PostgreSQL"):
            oxpg.connect(
                host="localhost",
                user="postgres",
                password="test",
                db="nonexistent_db_12345"
            )


class TestClientRepresentation:
    """Test Client __repr__ output"""

    def test_repr_contains_connection_info(self):
        """Should include host, port, db, and user in repr"""
        client = oxpg.connect(
            host="localhost",
            user="postgres",
            password="test",
            port=5432,
            db="test_db"
        )
        repr_str = repr(client)
        assert "localhost" in repr_str
        assert "5432" in repr_str
        assert "test_db" in repr_str
        assert "postgres" in repr_str

    def test_repr_does_not_expose_password(self):
        """Should not expose password in repr"""
        client = oxpg.connect(dsn=DSN)
        repr_str = repr(client)
        assert "password" not in repr_str.lower()

    def test_repr_format(self):
        """Should follow expected repr format"""
        client = oxpg.connect(dsn=DSN)
        repr_str = repr(client)
        assert repr_str.startswith("Client(")
        assert repr_str.endswith(")")
        assert "host=" in repr_str
        assert "port=" in repr_str
        assert "db=" in repr_str
        assert "user=" in repr_str


class TestSpecialCharactersInConnection:
    """Test handling of special characters in connection parameters"""

    @pytest.mark.skip(reason="Requires database setup with special characters")
    def test_database_name_with_hyphens_and_underscores(self):
        """Should handle database names with hyphens and underscores"""
        client = oxpg.connect(
            host="localhost",
            user="postgres",
            password="test",
            db="test-db_123"
        )
        assert "test-db_123" in repr(client)

    @pytest.mark.skip(reason="Requires user setup with percent-encoded password")
    def test_dsn_with_percent_encoded_password(self):
        """Should handle percent-encoded special characters in DSN password"""
        client = oxpg.connect(
            dsn="postgresql://testuser:p%40ss%3Aword@localhost:5432/postgres"
        )
        assert client is not None

    @pytest.mark.skip(reason="Requires user setup with special characters")
    def test_username_with_special_chars(self):
        """Should handle usernames with special characters"""
        client = oxpg.connect(
            host="localhost",
            user="user@domain",
            password="test",
            db="postgres"
        )
        assert "user@domain" in repr(client)
