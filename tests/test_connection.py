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

    def test_connect_dsn_and_individual_params(self):
        """Should raise ValueError when both DSN and individual params provided"""
        with pytest.raises(ValueError, match="Cannot specify both DSN"):
            oxpg.connect(
                dsn=DSN,
                host="localhost"
            )

    def test_connect_dsn_and_user(self):
        """Should raise ValueError when DSN and user provided"""
        with pytest.raises(ValueError, match="Cannot specify both DSN"):
            oxpg.connect(
                dsn=DSN,
                user="postgres"
            )

    def test_connect_dsn_and_password(self):
        """Should raise ValueError when DSN and password provided"""
        with pytest.raises(ValueError, match="Cannot specify both DSN"):
            oxpg.connect(
                dsn=DSN,
                password="test"
            )


class TestDSNParsing:
    """Test DSN parsing and validation"""

    def test_invalid_dsn_scheme(self):
        """Should raise ValueError for invalid scheme"""
        with pytest.raises(ValueError, match="must start with postgres"):
            oxpg.connect(dsn="mysql://user:pass@localhost/db")

    def test_invalid_dsn_missing_at(self):
        """Should raise ValueError when @ is missing"""
        with pytest.raises(ValueError, match="missing @ separating auth and host"):
            oxpg.connect(dsn="postgresql://localhost/db")

    def test_invalid_dsn_missing_colon(self):
        """Should raise ValueError when : is missing in auth"""
        with pytest.raises(ValueError, match="auth must be user:password"):
            oxpg.connect(dsn="postgresql://user@localhost/db")

    def test_invalid_dsn_missing_database(self):
        """Should raise ValueError when database name is missing"""
        with pytest.raises(ValueError, match="missing database name after"):
            oxpg.connect(dsn="postgresql://user:pass@localhost")

    def test_invalid_dsn_bad_port(self):
        """Should raise ValueError for non-numeric port"""
        with pytest.raises(ValueError, match="port must be a number"):
            oxpg.connect(dsn="postgresql://user:pass@localhost:abc/db")

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


class TestConnectionFailure:
    """Test actual connection failures"""

    def test_connect_to_invalid_host(self):
        """Should raise ConnectionError for invalid host"""
        with pytest.raises(ConnectionError, match="Failed to connect to PostgreSQL"):
            oxpg.connect(
                host="nonexistent.host.local",
                user="postgres",
                password="test"
            )

    def test_connect_with_wrong_credentials(self):
        """Should raise ConnectionError for wrong credentials"""
        with pytest.raises(ConnectionError, match="Failed to connect to PostgreSQL"):
            oxpg.connect(
                host="localhost",
                user="wronguser",
                password="wrongpass"
            )
