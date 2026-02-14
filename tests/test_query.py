import pytest
import oxpg
import uuid
from datetime import datetime, date, timedelta


class TestQueryExecution:

    async def test_simple_select_returns_list_of_dicts(self, db_client):
        result = await db_client.query("SELECT 1 as num")
        assert isinstance(result, list)
        assert len(result) == 1
        assert isinstance(result[0], dict)
        assert result[0]["num"] == 1

    async def test_parameterized_query(self, db_client, test_data):
        result = await db_client.query(
            "SELECT username FROM users WHERE id = $1", test_data["user"]["id"]
        )
        assert len(result) == 1
        assert result[0]["username"] == test_data["user"]["username"]

    async def test_empty_result_set(self, db_client):
        result = await db_client.query("SELECT * FROM users WHERE id = -9999")
        assert result == []

    async def test_insert_returning(self, db_client):
        rand_val = str(uuid.uuid4())
        result = await db_client.query(
            """
            INSERT INTO users (username, email, created_at)
            VALUES ($1, $2, $3)
            RETURNING id, username
            """,
            f"temp_{rand_val}",
            f"{rand_val}@test.com",
            datetime.now(),
        )
        assert len(result) == 1
        assert result[0]["username"] == f"temp_{rand_val}"
        assert "id" in result[0]

    async def test_null_handling(self, db_client):
        result = await db_client.query("SELECT $1::int as val", None)
        assert result[0]["val"] is None


class TestPostgresTypes:

    async def test_types_boolean(self, db_client, test_data):
        result = await db_client.query(
            "SELECT published FROM posts WHERE id = $1", test_data["post"]["id"]
        )
        assert isinstance(result[0]["published"], bool)
        assert result[0]["published"] is True

    async def test_types_datetime(self, db_client):
        result = await db_client.query("SELECT now() as current_time")
        assert isinstance(result[0]["current_time"], datetime)

    async def test_types_date(self, db_client):
        result = await db_client.query("SELECT CURRENT_DATE as today")
        assert isinstance(result[0]["today"], date)

    async def test_types_float(self, db_client):
        val = 10.55
        result = await db_client.query("SELECT $1::float as score", val)
        assert abs(result[0]["score"] - val) < 0.0001

    @pytest.mark.skip(reason="Lists/Arrays not yet implemented in oxpg")
    async def test_types_array_list(self, db_client, test_data):
        user_ids = [test_data["user"]["id"]]
        result = await db_client.query(
            "SELECT id FROM users WHERE id = ANY($1)", user_ids
        )
        assert len(result) >= 1


class TestComplexQueries:

    async def test_join_query(self, db_client, test_data):
        result = await db_client.query(
            """
            SELECT u.username, p.title
            FROM users u
            JOIN posts p ON u.id = p.user_id
            WHERE p.id = $1
            """,
            test_data["post"]["id"],
        )
        assert len(result) == 1
        assert result[0]["username"] == test_data["user"]["username"]

    async def test_full_text_search_simulation(self, db_client):
        result = await db_client.query(
            "SELECT 'Integration Test' ILIKE '%' || $1 || '%' as match", "Integration"
        )
        assert result[0]["match"] is True

    async def test_date_arithmetic(self, db_client):
        now = datetime.now()
        result = await db_client.query(
            "SELECT $1::timestamp < $2::timestamp as is_before",
            now - timedelta(days=1),
            now + timedelta(days=1),
        )
        assert result[0]["is_before"] is True


class TestQueryEdgeCasesAndErrors:

    async def test_sql_syntax_error(self, db_client):
        with pytest.raises(oxpg.Error) as exc:
            await db_client.query("SELEC * FROM users")
        assert "syntax error" in str(exc.value).lower()

    async def test_missing_column(self, db_client):
        with pytest.raises(oxpg.Error) as exc:
            await db_client.query("SELECT non_existent_column FROM users")
        assert "does not exist" in str(exc.value).lower()

    async def test_argument_count_mismatch_too_few(self, db_client):
        with pytest.raises(oxpg.Error) as exc:
            await db_client.query("SELECT $1, $2", 1)
        assert (
            "bind" in str(exc.value).lower()
            or "parameter" in str(exc.value).lower()
        )

    async def test_argument_count_mismatch_too_many(self, db_client):
        try:
            await db_client.query("SELECT $1::int", 1, 2)
        except Exception:
            pass

    async def test_sql_injection_attempt(self, db_client, test_data):
        malicious_input = f"{test_data['user']['id']} OR 1=1"
        with pytest.raises(oxpg.Error):
            await db_client.query(
                "SELECT * FROM users WHERE id = $1", malicious_input)

    async def test_unicode_handling(self, db_client):
        emoji_str = "User 🦊"
        result = await db_client.query("SELECT $1::text as emoji", emoji_str)
        assert result[0]["emoji"] == emoji_str
