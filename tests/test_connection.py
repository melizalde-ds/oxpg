import pytest
import oxpg

DSN = "postgresql://postgres:test@localhost:5432/postgres"


class TestExceptionHierarchy:

    def test_exceptions_exported(self):
        assert hasattr(oxpg, "Error")
        assert hasattr(oxpg, "InterfaceError")
        assert hasattr(oxpg, "OperationalError")
        assert hasattr(oxpg, "DatabaseError")

    def test_exception_inheritance(self):
        assert issubclass(oxpg.InterfaceError, oxpg.Error)
        assert issubclass(oxpg.OperationalError, oxpg.DatabaseError)
        assert issubclass(oxpg.DatabaseError, oxpg.Error)


class TestDSNParsing:

    def test_dsn_with_query_parameters(self):
        client = oxpg.connect(
            dsn="postgresql://postgres:test@localhost:5432/postgres?sslmode=disable"
        )
        assert client is not None

    def test_no_password_fails(self):
        with pytest.raises(oxpg.OperationalError) as exc:
            oxpg.connect(dsn="postgresql://postgres@localhost/postgres")
        assert "password missing" in str(exc.value).lower()

    def test_empty_dsn(self):
        with pytest.raises(oxpg.InterfaceError):
            oxpg.connect(dsn="")

    def test_malformed_dsn(self):
        with pytest.raises(oxpg.InterfaceError, match="invalid connection string"):
            oxpg.connect(dsn="not-a-valid-dsn")


class TestParameterValidation:

    def test_missing_host(self):
        with pytest.raises(oxpg.InterfaceError) as exc:
            oxpg.connect(user="postgres", password="test")
        assert "host" in str(exc.value).lower()

    def test_missing_user(self):
        with pytest.raises(oxpg.InterfaceError) as exc:
            oxpg.connect(host="localhost", password="test")
        assert "user" in str(exc.value).lower()

    def test_dsn_and_param_conflict(self):
        with pytest.raises(oxpg.InterfaceError, match="Cannot specify both DSN"):
            oxpg.connect(dsn=DSN, host="localhost")


class TestConnectivity:

    def test_connection_refused_port(self):
        with pytest.raises(oxpg.OperationalError) as exc:
            oxpg.connect(host="localhost", user="postgres",
                         password="test", port=1)
        assert "connection refused" in str(exc.value).lower()

    def test_database_not_found(self):
        with pytest.raises(oxpg.OperationalError) as exc:
            oxpg.connect(
                host="localhost", user="postgres", password="test",
                db="this_db_does_not_exist"
            )
        assert "does not exist" in str(exc.value).lower()

    def test_complex_hostname_lookup_failure(self):
        with pytest.raises(oxpg.OperationalError):
            oxpg.connect(
                dsn="postgresql://postgres:test@db-server.invalid:5432/db"
            )


class TestClientBehavior:

    def test_connect_returns_client(self):
        client = oxpg.connect(dsn=DSN)
        assert client is not None
        assert "postgres" in repr(client)

    def test_multiple_connections(self):
        c1 = oxpg.connect(dsn=DSN)
        c2 = oxpg.connect(dsn=DSN)
        assert c1 is not c2
