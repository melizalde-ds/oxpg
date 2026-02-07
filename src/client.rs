use pyo3::{PyErr, PyResult, pyclass, pyfunction};
use tokio_postgres::{Client as PgClient, Connection, Socket, tls::NoTlsStream};

#[pyclass]
struct Client {
    host: String,
    port: u16,
    db: String,
    user: String,
    client: PgClient,
    runtime: tokio::runtime::Runtime,
}

#[pyfunction]
#[pyo3(signature = (dsn=None, host=None, user=None, password=None, port=5432, db="postgres".to_string()))]
fn connect(
    dsn: Option<String>,
    host: Option<String>,
    user: Option<String>,
    password: Option<String>,
    port: u16,
    db: String,
) -> PyResult<Client> {
    let (host, user, port, db, connection_string) = match dsn {
        Some(s) => extract_host_from_dsn(s)?,
        None => {
            let host = host.ok_or_else(|| {
                PyErr::new::<pyo3::exceptions::PyValueError, _>("host is required")
            })?;
            let user = user.ok_or_else(|| {
                PyErr::new::<pyo3::exceptions::PyValueError, _>("user is required")
            })?;
            let password = password.ok_or_else(|| {
                PyErr::new::<pyo3::exceptions::PyValueError, _>("password is required")
            })?;
            (
                host.clone(),
                user.clone(),
                port,
                db.clone(),
                format!(
                    "postgresql://{}:{}@{}:{}/{}",
                    user, password, host, port, db
                ),
            )
        }
    };
    let runtime = tokio::runtime::Runtime::new().map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
            "Failed to create Tokio runtime: {}",
            e
        ))
    })?;

    let (client, connection) = runtime
        .block_on(async {
            tokio_postgres::connect(&connection_string, tokio_postgres::NoTls).await
        })
        .map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyConnectionError, _>(format!(
                "Failed to connect to database: {}",
                e
            ))
        })?;

    runtime.spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });

    Ok(Client {
        host,
        port,
        db,
        user,
        client,
        runtime,
    })
}

fn extract_host_from_dsn(dsn: String) -> PyResult<(String, String, u16, String, String)> {
    let without_scheme = dsn
        .strip_prefix("postgres://")
        .or_else(|| dsn.strip_prefix("postgresql://"))
        .ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Invalid DSN: must start with postgres:// or postgresql://",
            )
        })?;
    let (auth, rest) = without_scheme.split_once('@').ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "Invalid DSN: missing @ separating auth and host",
        )
    })?;
    let (user, _) = auth.split_once(':').ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyValueError, _>("Invalid DSN: auth must be user:password")
    })?;
    let (host_port, db) = rest.split_once('/').ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "Invalid DSN: missing database name after /",
        )
    })?;
    let (host, port) = match host_port.split_once(':') {
        Some((h, p)) => {
            let port = p.parse::<u16>().map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "Invalid DSN: port must be a number",
                )
            })?;
            (h.to_string(), port)
        }
        None => (host_port.to_string(), 5432),
    };
    Ok((host, user.to_string(), port, db.to_string(), dsn))
}
