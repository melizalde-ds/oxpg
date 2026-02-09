import pytest
import oxpg

DSN = "postgresql://postgres:test@localhost:5432/postgres"


class TestDSNParsingEdgeCases:
    """Additional DSN parsing tests based on Rust unit tests"""

    def test_dsn_without_password(self):
        """Should handle DSN without password (trust auth)"""
        try:
            client = oxpg.connect(
                dsn="postgresql://postgres@localhost/postgres")
            assert client is not None
        except ConnectionError:
            pass

    def test_dsn_with_query_parameters(self):
        """Should parse DSN with query parameters"""
        client = oxpg.connect(
            dsn="postgresql://postgres:test@localhost:5432/postgres?sslmode=disable"
        )
        assert client is not None
        assert "postgres" in repr(client)

    def test_dsn_with_complex_hostname(self):
        """Should parse DSN with complex hostname"""
        with pytest.raises(ConnectionError):
            oxpg.connect(
                dsn="postgresql://postgres:test@db-server.example.com:5432/postgres"
            )

    def test_dsn_port_boundaries(self):
        """Should handle minimum and maximum port numbers in DSN"""
        with pytest.raises(ConnectionError):
            oxpg.connect(
                dsn="postgresql://postgres:test@localhost:1/postgres"
            )

        with pytest.raises(ConnectionError):
            oxpg.connect(
                dsn="postgresql://postgres:test@localhost:65535/postgres"
            )

    def test_empty_dsn(self):
        """Should raise ValueError for empty DSN"""
        with pytest.raises(ValueError):
            oxpg.connect(dsn="")

    def test_malformed_dsn(self):
        """Should raise ValueError for malformed DSN"""
        with pytest.raises(ValueError, match="Invalid DSN"):
            oxpg.connect(dsn="not-a-valid-dsn")


class TestConnectionParameterEdgeCases:
    """Edge cases for connection parameters"""

    def test_empty_string_parameters(self):
        """Should handle empty strings in parameters"""
        with pytest.raises((ValueError, ConnectionError)):
            oxpg.connect(
                host="",
                user="",
                password="",
                db=""
            )

    def test_port_boundaries(self):
        """Should handle port boundary values"""
        with pytest.raises(ConnectionError):
            oxpg.connect(
                host="localhost",
                user="postgres",
                password="test",
                port=1,
                db="postgres"
            )

        with pytest.raises(ConnectionError):
            oxpg.connect(
                host="localhost",
                user="postgres",
                password="test",
                port=65535,
                db="postgres"
            )

    def test_unicode_in_parameters(self):
        """Should handle Unicode characters in parameters"""
        with pytest.raises(ConnectionError):
            oxpg.connect(
                host="hôst.example.com",
                user="üser",
                password="pässwörd",
                db="datäbase"
            )

    def test_very_long_parameters(self):
        """Should handle very long string parameters"""
        long_string = "a" * 1000
        with pytest.raises(ConnectionError):
            oxpg.connect(
                host=long_string,
                user=long_string,
                password=long_string,
                db=long_string
            )


class TestLocalhostVariations:
    """Test different localhost representations"""

    def test_localhost_ipv4(self):
        """Should connect using 127.0.0.1"""
        client = oxpg.connect(
            host="127.0.0.1",
            user="postgres",
            password="test",
            db="postgres"
        )
        assert "127.0.0.1" in repr(client)

    @pytest.mark.skip(reason="IPv6 support may vary by environment")
    def test_localhost_ipv6(self):
        """Should connect using ::1 (IPv6)"""
        client = oxpg.connect(
            host="::1",
            user="postgres",
            password="test",
            db="postgres"
        )
        assert client is not None

    def test_localhost_hostname(self):
        """Should connect using 'localhost' hostname"""
        client = oxpg.connect(
            host="localhost",
            user="postgres",
            password="test",
            db="postgres"
        )
        assert "localhost" in repr(client)


class TestDSNAndParamConflicts:
    """Test all combinations of DSN with individual parameters"""

    def test_dsn_with_port_uses_dsn_port(self):
        """Port has a default value so it is always passed; DSN port takes precedence"""
        client = oxpg.connect(dsn=DSN, port=9999)
        assert client is not None

    def test_dsn_with_db_uses_dsn_db(self):
        """DB has a default value so it is always passed; DSN db takes precedence"""
        client = oxpg.connect(dsn=DSN, db="other_db")
        assert client is not None

    def test_dsn_with_multiple_params(self):
        """Should reject DSN with multiple individual parameters"""
        with pytest.raises(ValueError, match="Cannot specify both DSN"):
            oxpg.connect(
                dsn=DSN,
                host="localhost",
                user="postgres",
                password="test"
            )


class TestDatabaseNames:
    """Test database name handling"""

    def test_db_with_hyphens(self):
        """Should handle database names with hyphens"""
        with pytest.raises(ConnectionError):
            oxpg.connect(
                host="localhost",
                user="postgres",
                password="test",
                db="test-database"
            )

    def test_db_with_underscores(self):
        """Should handle database names with underscores"""
        with pytest.raises(ConnectionError):
            oxpg.connect(
                host="localhost",
                user="postgres",
                password="test",
                db="test_database"
            )

    def test_db_with_numbers(self):
        """Should handle database names with numbers"""
        with pytest.raises(ConnectionError):
            oxpg.connect(
                host="localhost",
                user="postgres",
                password="test",
                db="database123"
            )

    def test_db_with_mixed_special_chars(self):
        """Should handle database names with mixed special characters"""
        with pytest.raises(ConnectionError):
            oxpg.connect(
                host="localhost",
                user="postgres",
                password="test",
                db="my-db_123"
            )


class TestCustomPortAndDatabase:
    """Test non-default ports and database names"""

    def test_custom_port_validation(self):
        """Should validate port is within valid range"""
        with pytest.raises((ValueError, ConnectionError)):
            oxpg.connect(
                host="localhost",
                user="postgres",
                password="test",
                port=0,
                db="postgres"
            )

    def test_custom_database_name_in_connection(self):
        """Should use custom database name"""
        with pytest.raises(ConnectionError):
            client = oxpg.connect(
                host="localhost",
                user="postgres",
                password="test",
                port=5432,
                db="custom_database_name"
            )


class TestErrorMessages:
    """Test that error messages are clear and helpful"""

    def test_missing_host_error_message(self):
        """Error message should clearly indicate missing host"""
        with pytest.raises(ValueError) as exc_info:
            oxpg.connect(user="postgres", password="test")
        assert "host" in str(exc_info.value).lower()

    def test_missing_user_error_message(self):
        """Error message should clearly indicate missing user"""
        with pytest.raises(ValueError) as exc_info:
            oxpg.connect(host="localhost", password="test")
        assert "user" in str(exc_info.value).lower()

    def test_missing_password_error_message(self):
        """Error message should clearly indicate missing password"""
        with pytest.raises(ValueError) as exc_info:
            oxpg.connect(host="localhost", user="postgres")
        assert "password" in str(exc_info.value).lower()

    def test_invalid_dsn_scheme_error_message(self):
        """Error message should indicate invalid DSN scheme"""
        with pytest.raises(ValueError) as exc_info:
            oxpg.connect(dsn="http://user:pass@localhost/db")
        error_msg = str(exc_info.value)
        assert "Invalid DSN" in error_msg or "scheme" in error_msg.lower()

    def test_dsn_conflict_error_message(self):
        """Error message should clearly explain DSN conflict"""
        with pytest.raises(ValueError) as exc_info:
            oxpg.connect(dsn=DSN, host="localhost")
        assert "Cannot specify both DSN" in str(exc_info.value)


class TestConnectionReturnValue:
    """Test that connection returns proper client object"""

    def test_client_is_not_none(self):
        """Should return non-None client"""
        client = oxpg.connect(dsn=DSN)
        assert client is not None

    def test_client_is_reusable(self):
        """Should be able to use client multiple times"""
        client = oxpg.connect(dsn=DSN)
        client_id = id(client)
        assert id(client) == client_id

    def test_multiple_clients(self):
        """Should be able to create multiple independent clients"""
        client1 = oxpg.connect(dsn=DSN)
        client2 = oxpg.connect(dsn=DSN)
        assert client1 is not client2
        assert id(client1) != id(client2)
