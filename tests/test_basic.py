import pytest
import oxpg

DSN = "postgresql://postgres:test@localhost:5432/postgres"


def test_connect_dsn():
    client = oxpg.connect(DSN)
    assert client is not None


def test_connect_params():
    client = oxpg.connect(host="localhost", user="postgres", password="test")
    assert client is not None


def test_connect_missing_host():
    with pytest.raises(ValueError):
        oxpg.connect(user="postgres", password="test")
