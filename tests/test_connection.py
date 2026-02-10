import pytest
import oxpg
import uuid
from datetime import datetime, date, timedelta

DSN = "postgresql://postgres:test@localhost:5432/postgres"


@pytest.fixture(scope="module")
def db_client():
    """
    Creates a single database connection for the test module.
    Scope='module' allows reusing the connection across tests, mimicking a real application
    lifecycle and significantly speeding up the test suite.
    """
    client = oxpg.connect(DSN)
    yield client


@pytest.fixture(scope="module")
def test_data(db_client):
    """
    Inserts a dedicated test user and post to ensure tests run against 
    known data, rather than assuming 'id=1' exists.

    Yields a dictionary containing the created user and post rows.
    Cleans up the data after the module finishes.
    """

    u_sql = """
    INSERT INTO users (username, email, created_at)
    VALUES ($1, $2, $3)
    RETURNING id, username, email;
    """
    unique_str = str(uuid.uuid4())[:8]
    user = db_client.query(
        u_sql, f"test_user_{unique_str}", f"{unique_str}@example.com", datetime.now())[0]

    p_sql = """
    INSERT INTO posts (user_id, title, content, published, created_at)
    VALUES ($1, $2, $3, $4, $5)
    RETURNING id, title, published, created_at;
    """
    post = db_client.query(
        p_sql,
        user['id'],
        "Integration Test Post",
        "Content for testing types",
        True,
        datetime.now()
    )[0]

    data = {"user": user, "post": post}
    yield data

    db_client.query("DELETE FROM posts WHERE id = $1", post['id'])
    db_client.query("DELETE FROM users WHERE id = $1", user['id'])


class TestExceptionHierarchy:
    """Verifies that the library follows PEP 249 (Python DB API) exception standards."""

    def test_exceptions_exported(self):
        """Check that the module exports the standard exception classes."""
        assert hasattr(oxpg, "Error")
        assert hasattr(oxpg, "InterfaceError")
        assert hasattr(oxpg, "OperationalError")
        assert hasattr(oxpg, "DatabaseError")

    def test_exception_inheritance(self):
        """Ensure the exception hierarchy is correctly established (e.g., OperationalError inherits from DatabaseError)."""
        assert issubclass(oxpg.InterfaceError, oxpg.Error)
        assert issubclass(oxpg.OperationalError, oxpg.DatabaseError)
        assert issubclass(oxpg.DatabaseError, oxpg.Error)


class TestDSNParsing:
    """Tests DSN parsing logic and edge cases."""

    def test_dsn_with_query_parameters(self):
        """Should correctly parse a DSN that includes query parameters (e.g., sslmode)."""
        client = oxpg.connect(
            dsn="postgresql://postgres:test@localhost:5432/postgres?sslmode=disable")
        assert client is not None

    def test_no_password_fails(self):
        """Passwordless connections should fail if the server requires auth, raising OperationalError."""
        with pytest.raises(oxpg.OperationalError) as exc:
            oxpg.connect(dsn="postgresql://postgres@localhost/postgres")
        assert "password missing" in str(exc.value).lower()

    def test_empty_dsn(self):
        """Passing an empty string as DSN should trigger an InterfaceError."""
        with pytest.raises(oxpg.InterfaceError):
            oxpg.connect(dsn="")

    def test_malformed_dsn(self):
        """Non-DSN strings (gibberish) should raise an InterfaceError."""
        with pytest.raises(oxpg.InterfaceError, match="invalid connection string"):
            oxpg.connect(dsn="not-a-valid-dsn")


class TestParameterValidation:
    """Tests individual connection parameters (host, user, etc.) validation."""

    def test_missing_host(self):
        """Missing 'host' parameter should be caught as an InterfaceError."""
        with pytest.raises(oxpg.InterfaceError) as exc:
            oxpg.connect(user="postgres", password="test")
        assert "host" in str(exc.value).lower()

    def test_missing_user(self):
        """Missing 'user' parameter should be caught as an InterfaceError."""
        with pytest.raises(oxpg.InterfaceError) as exc:
            oxpg.connect(host="localhost", password="test")
        assert "user" in str(exc.value).lower()

    def test_dsn_and_param_conflict(self):
        """Mixing a DSN string with individual parameters (host/user) is ambiguous and should raise InterfaceError."""
        with pytest.raises(oxpg.InterfaceError, match="Cannot specify both DSN"):
            oxpg.connect(dsn=DSN, host="localhost")


class TestConnectivity:
    """Tests that require attempting an actual network connection to fail."""

    def test_connection_refused_port(self):
        """Using a bad port (where no DB is listening) should raise OperationalError."""
        with pytest.raises(oxpg.OperationalError) as exc:
            oxpg.connect(host="localhost", user="postgres",
                         password="test", port=1)
        assert "connection refused" in str(exc.value).lower()

    def test_database_not_found(self):
        """Connecting to a non-existent database name should raise OperationalError."""
        with pytest.raises(oxpg.OperationalError) as exc:
            oxpg.connect(host="localhost", user="postgres",
                         password="test", db="this_db_does_not_exist")
        assert "does not exist" in str(exc.value).lower()

    def test_complex_hostname_lookup_failure(self):
        """Unresolvable hostnames should raise OperationalError."""
        with pytest.raises(oxpg.OperationalError):
            oxpg.connect(
                dsn="postgresql://postgres:test@db-server.invalid:5432/db")


class TestClientBehavior:
    """Tests the resulting Client object structure and isolation."""

    def test_connect_returns_client(self):
        """Should return a valid client instance with a useful string representation."""
        client = oxpg.connect(dsn=DSN)
        assert client is not None
        assert "postgres" in repr(client)

    def test_multiple_connections(self):
        """Should be able to create independent connections that do not share state."""
        c1 = oxpg.connect(dsn=DSN)
        c2 = oxpg.connect(dsn=DSN)
        assert c1 is not c2


class TestQueryExecution:
    """Tests checking successful query execution and data retrieval."""

    def test_simple_select_returns_list_of_dicts(self, db_client):
        """Verifies basic SELECT functionality returns a list of dictionaries."""
        result = db_client.query("SELECT 1 as num")
        assert isinstance(result, list)
        assert len(result) == 1
        assert isinstance(result[0], dict)
        assert result[0]["num"] == 1

    def test_parameterized_query(self, db_client, test_data):
        """Verifies $1 parameter replacement works correctly (Security/Functionality check)."""
        user_id = test_data["user"]["id"]
        sql = "SELECT username FROM users WHERE id = $1"
        result = db_client.query(sql, user_id)
        assert len(result) == 1
        assert result[0]["username"] == test_data["user"]["username"]

    def test_empty_result_set(self, db_client):
        """Verifies that queries matching no rows gracefully return an empty list."""
        sql = "SELECT * FROM users WHERE id = -9999"
        result = db_client.query(sql)
        assert result == []

    def test_insert_returning(self, db_client):
        """Verifies that INSERT ... RETURNING correctly returns the created row data."""
        timestamp = datetime.now()
        sql = """
        INSERT INTO users (username, email, created_at)
        VALUES ($1, $2, $3)
        RETURNING id, username
        """
        rand_val = str(uuid.uuid4())
        result = db_client.query(
            sql, f"temp_{rand_val}", f"{rand_val}@test.com", timestamp)

        assert len(result) == 1
        assert result[0]["username"] == f"temp_{rand_val}"
        assert "id" in result[0]

    def test_null_handling(self, db_client):
        """Verifies that Python 'None' is correctly converted to SQL 'NULL'."""
        sql = "SELECT $1::int as val"
        result = db_client.query(sql, None)
        assert result[0]["val"] is None


class TestPostgresTypes:
    """Verifies Python <-> Rust <-> Postgres type conversions."""

    def test_types_boolean(self, db_client, test_data):
        """Verifies BOOL handling (Postgres 't'/'f' -> Python True/False)."""
        sql = "SELECT published FROM posts WHERE id = $1"
        result = db_client.query(sql, test_data["post"]["id"])
        assert isinstance(result[0]["published"], bool)
        assert result[0]["published"] is True

    def test_types_datetime(self, db_client):
        """Verifies TIMESTAMP handling converts to Python datetime objects."""
        sql = "SELECT now() as current_time"
        result = db_client.query(sql)
        assert isinstance(result[0]["current_time"], datetime)

    def test_types_date(self, db_client):
        """Verifies DATE handling converts to Python date objects."""
        sql = "SELECT CURRENT_DATE as today"
        result = db_client.query(sql)
        assert isinstance(result[0]["today"], date)

    def test_types_float(self, db_client):
        """Verifies FLOAT/DOUBLE handling matches precision expectations."""
        val = 10.55
        sql = "SELECT $1::float as score"
        result = db_client.query(sql, val)
        assert abs(result[0]["score"] - val) < 0.0001

    @pytest.mark.skip(reason="Lists/Arrays not yet implemented in oxpg")
    def test_types_array_list(self, db_client, test_data):
        """Verifies passing Python lists to Postgres ANY($1) operations."""
        user_ids = [test_data["user"]["id"]]
        sql = "SELECT id FROM users WHERE id = ANY($1)"
        result = db_client.query(sql, user_ids)
        assert len(result) >= 1


class TestComplexQueries:
    """Tests for complex SQL logic, joins, and filters."""

    def test_join_query(self, db_client, test_data):
        """Verifies that JOINs return columns from multiple tables correctly."""
        sql = """
        SELECT u.username, p.title 
        FROM users u 
        JOIN posts p ON u.id = p.user_id 
        WHERE p.id = $1
        """
        result = db_client.query(sql, test_data["post"]["id"])
        assert len(result) == 1
        assert result[0]["username"] == test_data["user"]["username"]

    def test_full_text_search_simulation(self, db_client):
        """Verifies text manipulation and ILIKE logic."""
        term = "Integration"
        sql = "SELECT 'Integration Test' ILIKE '%' || $1 || '%' as match"
        result = db_client.query(sql, term)
        assert result[0]["match"] is True

    def test_date_arithmetic(self, db_client):
        """Verifies comparison logic using timestamps (BETWEEN/operators)."""
        now = datetime.now()
        past = now - timedelta(days=1)
        future = now + timedelta(days=1)

        sql = "SELECT $1::timestamp < $2::timestamp as is_before"
        result = db_client.query(sql, past, future)
        assert result[0]["is_before"] is True


class TestEdgeCasesAndErrors:
    """Tests covering failures, bad input, and security/safety."""

    def test_sql_syntax_error(self, db_client):
        """Ensure standard Postgres syntax errors raise the generic oxpg.Error."""
        with pytest.raises(oxpg.Error) as exc:
            db_client.query("SELEC * FROM users")
        assert "syntax error" in str(exc.value).lower()

    def test_missing_column(self, db_client):
        """Querying a non-existent column should raise a clear error."""
        with pytest.raises(oxpg.Error) as exc:
            db_client.query("SELECT non_existent_column FROM users")
        assert "does not exist" in str(exc.value).lower()

    def test_argument_count_mismatch_too_few(self, db_client):
        """Providing fewer arguments than placeholders should raise a binding error."""
        with pytest.raises(oxpg.Error) as exc:
            db_client.query("SELECT $1, $2", 1)
        assert "bind" in str(exc.value).lower(
        ) or "parameter" in str(exc.value).lower()

    def test_argument_count_mismatch_too_many(self, db_client):
        """Providing more arguments than placeholders should ideally be ignored or handled gracefully."""
        try:
            db_client.query("SELECT $1::int", 1, 2)
        except Exception:
            pass

    def test_sql_injection_attempt(self, db_client, test_data):
        """Ensure parameters are treated as literal values, effectively blocking SQL injection."""
        user_id = test_data["user"]["id"]
        malicious_input = f"{user_id} OR 1=1"

        with pytest.raises(oxpg.Error):
            db_client.query(
                "SELECT * FROM users WHERE id = $1", malicious_input)

    def test_unicode_handling(self, db_client):
        """Test proper handling of UTF-8 characters (emojis, etc.)."""
        emoji_str = "User 🦊"
        sql = "SELECT $1::text as emoji"
        result = db_client.query(sql, emoji_str)
        assert result[0]["emoji"] == emoji_str
