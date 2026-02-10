import pytest
import oxpg

DSN = "postgresql://postgres:test@localhost:5432/postgres"


class TestExceptionHierarchy:
    """Verifies that the library follows PEP 249 standards"""

    def test_exceptions_exported(self):
        """Check that Pylance/Runtime see the custom exceptions"""
        assert hasattr(oxpg, "Error")
        assert hasattr(oxpg, "InterfaceError")
        assert hasattr(oxpg, "OperationalError")
        assert hasattr(oxpg, "DatabaseError")

    def test_exception_inheritance(self):
        """Ensure the hierarchy is correctly established"""
        assert issubclass(oxpg.InterfaceError, oxpg.Error)
        assert issubclass(oxpg.OperationalError, oxpg.DatabaseError)
        assert issubclass(oxpg.DatabaseError, oxpg.Error)


class TestDSNParsing:
    """Tests DSN parsing and edge cases"""

    def test_dsn_with_query_parameters(self):
        """Should parse DSN with query parameters correctly"""
        client = oxpg.connect(
            dsn="postgresql://postgres:test@localhost:5432/postgres?sslmode=disable"
        )
        assert client is not None

    def test_no_password_fails(self):
        """Passwordless is currently unsupported; should raise OperationalError"""
        with pytest.raises(oxpg.OperationalError) as exc:
            oxpg.connect(dsn="postgresql://postgres@localhost/postgres")
        assert "password missing" in str(exc.value).lower()

    def test_empty_dsn(self):
        """Empty DSN should trigger a missing parameter InterfaceError"""
        with pytest.raises(oxpg.InterfaceError):
            oxpg.connect(dsn="")

    def test_malformed_dsn(self):
        """Non-DSN strings should raise InterfaceError"""
        with pytest.raises(oxpg.InterfaceError, match="invalid connection string"):
            oxpg.connect(dsn="not-a-valid-dsn")


class TestParameterValidation:
    """Tests individual connection parameters"""

    def test_missing_host(self):
        """Missing host should be caught as an InterfaceError"""
        with pytest.raises(oxpg.InterfaceError) as exc:
            oxpg.connect(user="postgres", password="test")
        assert "host" in str(exc.value).lower()

    def test_missing_user(self):
        """Missing user should be caught as an InterfaceError"""
        with pytest.raises(oxpg.InterfaceError) as exc:
            oxpg.connect(host="localhost", password="test")
        assert "user" in str(exc.value).lower()

    def test_dsn_and_param_conflict(self):
        """Mixing DSN and host/user should raise an InterfaceError"""
        with pytest.raises(oxpg.InterfaceError, match="Cannot specify both DSN"):
            oxpg.connect(dsn=DSN, host="localhost")


class TestConnectivity:
    """Tests that require an actual (or attempted) network connection"""

    def test_connection_refused_port(self):
        """Using a bad port should raise OperationalError"""
        with pytest.raises(oxpg.OperationalError) as exc:
            oxpg.connect(host="localhost", user="postgres",
                         password="test", port=1)
        assert "connection refused" in str(exc.value).lower()

    def test_database_not_found(self):
        """Postgres FATAL error (DB not found) should be an OperationalError"""
        with pytest.raises(oxpg.OperationalError) as exc:
            oxpg.connect(
                host="localhost",
                user="postgres",
                password="test",
                db="this_db_does_not_exist"
            )
        assert "does not exist" in str(exc.value).lower()

    def test_complex_hostname_lookup_failure(self):
        """Unresolvable hostnames should raise OperationalError"""
        with pytest.raises(oxpg.OperationalError):
            oxpg.connect(
                dsn="postgresql://postgres:test@db-server.invalid:5432/db")


class TestClientBehavior:
    """Tests the resulting Client object"""

    def test_connect_returns_client(self):
        """Should return a valid client instance on success"""
        client = oxpg.connect(dsn=DSN)
        assert client is not None
        assert "postgres" in repr(client)

    def test_multiple_connections(self):
        """Should be able to create independent connections"""
        c1 = oxpg.connect(dsn=DSN)
        c2 = oxpg.connect(dsn=DSN)
        assert c1 is not c2
