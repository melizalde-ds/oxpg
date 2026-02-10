# oxpg

A fast PostgreSQL client for Python, built with Rust and [tokio-postgres](https://docs.rs/tokio-postgres).

## Installation

```bash
pip install oxpg
```

**Requires Python 3.14+**

## Quick Start

```python
import oxpg

# Connect via DSN
client = oxpg.connect("postgresql://user:password@localhost:5432/mydb")

# Or with individual parameters
client = oxpg.connect(host="localhost", user="myuser", password="mypass", db="mydb")

# Query — returns a list of dicts
rows = client.query("SELECT id, username FROM users WHERE id = $1", 1)
print(rows[0]["username"])

# Execute — returns affected row count
count = client.execute("UPDATE users SET active = $1 WHERE id = $2", True, 1)
print(f"{count} row(s) affected")

# INSERT ... RETURNING — use query()
row = client.query(
    "INSERT INTO users (username, email) VALUES ($1, $2) RETURNING id",
    "alice", "alice@example.com"
)[0]
print(row["id"])
```

## API

### `oxpg.connect(...) -> Client`

| Parameter  | Type          | Default      | Description            |
| ---------- | ------------- | ------------ | ---------------------- |
| `dsn`      | `str \| None` | `None`       | Full connection string |
| `host`     | `str \| None` | `None`       | Hostname or IP         |
| `user`     | `str \| None` | `None`       | Database user          |
| `password` | `str \| None` | `None`       | Password               |
| `port`     | `int`         | `5432`       | Port                   |
| `db`       | `str`         | `"postgres"` | Database name          |

Either `dsn` or the individual parameters (`host`, `user`, `password`) must be provided — not both.

### `Client.query(sql, *args) -> list[dict]`

Executes a SQL statement and returns all rows as a list of dictionaries. Use this for `SELECT` and `INSERT/UPDATE/DELETE ... RETURNING`.

### `Client.execute(sql, *args) -> int`

Executes a SQL statement and returns the number of affected rows. Use this for `INSERT`, `UPDATE`, and `DELETE` without `RETURNING`.

### Supported Parameter Types

| Python      | PostgreSQL                 |
| ----------- | -------------------------- |
| `bool`      | `BOOL`                     |
| `int`       | `INT2`, `INT4`, `INT8`     |
| `float`     | `FLOAT4`, `FLOAT8`         |
| `str`       | `TEXT`, `VARCHAR`          |
| `bytes`     | `BYTEA`                    |
| `datetime`  | `TIMESTAMP`, `TIMESTAMPTZ` |
| `date`      | `DATE`                     |
| `time`      | `TIME`                     |
| `timedelta` | `INTERVAL`                 |
| `None`      | `NULL`                     |

### Exceptions

oxpg follows the [PEP 249](https://peps.python.org/pep-0249/) exception hierarchy:

```
oxpg.Error
├── oxpg.InterfaceError      # Bad parameters, invalid DSN
└── oxpg.DatabaseError       # Database-level errors
    ├── oxpg.OperationalError  # Connection failures
    ├── oxpg.DataError         # Type conversion failures
    └── oxpg.InternalError     # Unexpected internal errors
```

```python
try:
    client = oxpg.connect(dsn="postgresql://bad:creds@localhost/db")
except oxpg.OperationalError as e:
    print(f"Could not connect: {e}")

try:
    client.query("SELEC * FROM users")
except oxpg.DatabaseError as e:
    print(f"Query failed: {e}")
```

## Known Limitations

**Single connection per `Client`** — `oxpg` does not implement connection pooling. Each `Client` instance holds exactly one connection. For scripts, batch jobs, and single-threaded applications this is fine. If you need pooling for a web server or concurrent workload, manage multiple `Client` instances yourself or wait for a future release.

**Synchronous only** — there is no `async`/`await` API. All calls block the calling thread. Async support is planned for a future release.

**`NUMERIC`/`DECIMAL` columns** — these are returned as `str` to avoid precision loss. If you need arithmetic, cast in Python: `from decimal import Decimal; value = Decimal(row["price"])`. Alternatively, cast in SQL: `SELECT price::float FROM products`.

**No array/list support** — PostgreSQL array types (`INT[]`, `TEXT[]`, etc.) are not yet supported as parameters or return values.

**No transaction API** — transactions can be managed manually by passing `BEGIN`, `COMMIT`, and `ROLLBACK` through `execute()`. A `with client.transaction():` context manager is planned.

## Development

> [!NOTE]
> Assumes you have Rust and Python 3.14+ installed, using uv

```bash
# Start test database
docker compose -f docker-compose.test.yaml up -d

# Install dev dependencies
uv sync

# Build, Sub module, and uv sync
./build.sh

# Run tests
cargo test --release
pytest tests/
```

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT License ([LICENSE-MIT](LICENSE-MIT))

at your option.
