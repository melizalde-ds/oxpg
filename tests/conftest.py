import pytest
import oxpg
import uuid
from datetime import datetime

DSN = "postgresql://postgres:test@localhost:5432/postgres"


@pytest.fixture(scope="module")
def db_client():
    client = oxpg.connect(DSN)
    yield client


@pytest.fixture(scope="module")
def test_data(db_client):
    unique_str = str(uuid.uuid4())[:8]

    user = db_client.query(
        """
        INSERT INTO users (username, email, created_at)
        VALUES ($1, $2, $3)
        RETURNING id, username, email
        """,
        f"test_user_{unique_str}",
        f"{unique_str}@example.com",
        datetime.now(),
    )[0]

    post = db_client.query(
        """
        INSERT INTO posts (user_id, title, content, published, created_at)
        VALUES ($1, $2, $3, $4, $5)
        RETURNING id, title, published, created_at
        """,
        user["id"],
        "Integration Test Post",
        "Content for testing types",
        True,
        datetime.now(),
    )[0]

    yield {"user": user, "post": post}

    db_client.query("DELETE FROM posts WHERE id = $1", post["id"])
    db_client.query("DELETE FROM users WHERE id = $1", user["id"])
