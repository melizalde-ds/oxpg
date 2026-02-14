import pytest
import oxpg
import uuid
from datetime import datetime


class TestExecuteBasics:

    def test_execute_returns_int(self, db_client):
        rows = db_client.execute("SELECT 1")
        assert isinstance(rows, int)

    def test_execute_insert_returns_one(self, db_client):
        rand = str(uuid.uuid4())
        rows = db_client.execute(
            """
            INSERT INTO users (username, email, created_at)
            VALUES ($1, $2, $3)
            """,
            f"tmp_{rand}",
            f"{rand}@tmp.com",
            datetime.now(),
        )
        assert rows == 1
        db_client.execute(
            "DELETE FROM users WHERE username = $1", f"tmp_{rand}")

    def test_execute_update_returns_correct_count(self, db_client, test_data):
        rows = db_client.execute(
            "UPDATE posts SET published = $1 WHERE id = $2",
            True,
            test_data["post"]["id"],
        )
        assert rows == 1

    def test_execute_update_no_match_returns_zero(self, db_client):
        rows = db_client.execute(
            "UPDATE users SET username = $1 WHERE id = $2",
            "ghost",
            -9999,
        )
        assert rows == 0

    def test_execute_delete_returns_correct_count(self, db_client):
        rand = str(uuid.uuid4())
        db_client.execute(
            """
            INSERT INTO users (username, email, created_at)
            VALUES ($1, $2, $3)
            """,
            f"del_{rand}",
            f"{rand}@del.com",
            datetime.now(),
        )
        rows = db_client.execute(
            "DELETE FROM users WHERE username = $1", f"del_{rand}")
        assert rows == 1

    def test_execute_delete_no_match_returns_zero(self, db_client):
        rows = db_client.execute("DELETE FROM users WHERE id = $1", -9999)
        assert rows == 0

    def test_execute_multi_row_update(self, db_client, test_data):
        rand = str(uuid.uuid4())[:6]
        batch_tag = f"batch_{rand}"

        for i in range(3):
            db_client.execute(
                """
                INSERT INTO users (username, email, created_at)
                VALUES ($1, $2, $3)
                """,
                f"{batch_tag}_{i}",
                f"{batch_tag}_{i}@bulk.com",
                datetime.now(),
            )

        rows = db_client.execute(
            "UPDATE users SET username = username || '_updated' WHERE username LIKE $1",
            f"{batch_tag}_%",
        )
        assert rows == 3

        db_client.execute(
            "DELETE FROM users WHERE username LIKE $1", f"{batch_tag}_%")


class TestExecuteVsQuery:

    def test_execute_does_not_return_rows(self, db_client, test_data):
        result = db_client.execute(
            "UPDATE posts SET published = $1 WHERE id = $2",
            True,
            test_data["post"]["id"],
        )
        assert not isinstance(result, list)
        assert isinstance(result, int)

    async def test_query_select_execute_select_same_result(self, db_client):
        q_result = await db_client.query("SELECT 1 AS x")
        assert isinstance(q_result, list)

        e_result = db_client.execute("SELECT 1 AS x")
        assert isinstance(e_result, int)
        assert e_result >= 0

    async def test_insert_returning_use_query_not_execute(self, db_client):
        rand = str(uuid.uuid4())
        rows = await db_client.query(
            """
            INSERT INTO users (username, email, created_at)
            VALUES ($1, $2, $3)
            RETURNING id
            """,
            f"ret_{rand}",
            f"{rand}@ret.com",
            datetime.now(),
        )
        assert len(rows) == 1
        assert "id" in rows[0]

        db_client.execute("DELETE FROM users WHERE id = $1", rows[0]["id"])


class TestExecuteParameterBinding:

    def test_execute_with_null_parameter(self, db_client, test_data):
        rows = db_client.execute(
            "UPDATE posts SET content = $1 WHERE id = $2",
            None,
            test_data["post"]["id"],
        )
        assert rows == 1

    async def test_execute_unicode_parameter(self, db_client):
        rand = str(uuid.uuid4())
        emoji_name = f"🦊_{rand}"
        db_client.execute(
            """
            INSERT INTO users (username, email, created_at)
            VALUES ($1, $2, $3)
            """,
            emoji_name,
            f"{rand}@fox.com",
            datetime.now(),
        )
        result = await db_client.query(
            "SELECT username FROM users WHERE username = $1", emoji_name
        )
        assert result[0]["username"] == emoji_name
        db_client.execute("DELETE FROM users WHERE username = $1", emoji_name)

    def test_execute_injection_attempt_raises(self, db_client, test_data):
        malicious = f"{test_data['user']['id']} OR 1=1"
        with pytest.raises(oxpg.Error):
            db_client.execute("DELETE FROM users WHERE id = $1", malicious)


class TestExecuteErrors:

    def test_execute_syntax_error_raises(self, db_client):
        with pytest.raises(oxpg.Error) as exc:
            db_client.execute("DELET FROM users WHERE id = $1", 1)
        assert "syntax error" in str(exc.value).lower()

    def test_execute_missing_column_raises(self, db_client):
        with pytest.raises(oxpg.Error) as exc:
            db_client.execute(
                "UPDATE users SET ghost_column = $1 WHERE id = $2", "x", 1
            )
        assert "does not exist" in str(exc.value).lower()

    def test_execute_too_few_args_raises(self, db_client):
        with pytest.raises(oxpg.Error) as exc:
            db_client.execute(
                "UPDATE users SET username = $1 WHERE id = $2", "only_one"
            )
        assert (
            "bind" in str(exc.value).lower()
            or "parameter" in str(exc.value).lower()
        )

    def test_execute_too_many_args_handled_gracefully(self, db_client):
        try:
            db_client.execute("SELECT $1::int", 1, 999)
        except Exception:
            pass

    def test_execute_foreign_key_violation_raises(self, db_client):
        with pytest.raises(oxpg.Error):
            db_client.execute(
                """
                INSERT INTO posts (user_id, title, content, published, created_at)
                VALUES ($1, $2, $3, $4, $5)
                """,
                -9999,
                "Orphan Post",
                "content",
                False,
                datetime.now(),
            )

    def test_execute_unique_violation_raises(self, db_client):
        rand = str(uuid.uuid4())
        username = f"unique_{rand}"

        db_client.execute(
            """
            INSERT INTO users (username, email, created_at)
            VALUES ($1, $2, $3)
            """,
            username, f"{rand}@uniq.com", datetime.now(),
        )

        with pytest.raises(oxpg.Error):
            db_client.execute(
                """
                INSERT INTO users (username, email, created_at)
                VALUES ($1, $2, $3)
                """,
                username,
                f"other_{rand}@uniq.com",
                datetime.now(),
            )

        db_client.execute("DELETE FROM users WHERE username = $1", username)


class TestExecuteIdempotency:

    async def test_execute_multiple_times_same_client(self, db_client):
        rand = str(uuid.uuid4())[:6]
        for i in range(5):
            db_client.execute(
                """
                INSERT INTO users (username, email, created_at)
                VALUES ($1, $2, $3)
                """,
                f"repeat_{rand}_{i}",
                f"{rand}_{i}@repeat.com",
                datetime.now(),
            )

        count = await db_client.query(
            "SELECT COUNT(*) AS n FROM users WHERE username LIKE $1",
            f"repeat_{rand}_%",
        )
        assert count[0]["n"] == 5

        db_client.execute(
            "DELETE FROM users WHERE username LIKE $1", f"repeat_{rand}_%")

    async def test_execute_after_query_works(self, db_client, test_data):
        _ = await db_client.query(
            "SELECT id FROM users WHERE id = $1", test_data["user"]["id"]
        )
        rows = db_client.execute(
            "UPDATE posts SET published = $1 WHERE id = $2",
            True,
            test_data["post"]["id"],
        )
        assert isinstance(rows, int)

    async def test_query_after_execute_works(self, db_client, test_data):
        new_title = f"Updated Title {uuid.uuid4()}"
        db_client.execute(
            "UPDATE posts SET title = $1 WHERE id = $2",
            new_title,
            test_data["post"]["id"],
        )
        result = await db_client.query(
            "SELECT title FROM posts WHERE id = $1", test_data["post"]["id"]
        )
        assert result[0]["title"] == new_title
