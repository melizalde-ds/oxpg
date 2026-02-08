use crate::errors::OxpgError;
use pyo3::{PyErr, PyResult, pyclass, pyfunction, pymethods};
use pyo3_stub_gen::derive::*;
use tokio_postgres::Client as PgClient;

#[gen_stub_pyclass]
#[pyclass]
pub struct Client {
    host: String,
    port: u16,
    db: String,
    user: String,
    client: PgClient,
    runtime: tokio::runtime::Runtime,
}

#[gen_stub_pymethods]
#[pymethods]
impl Client {
    fn __repr__(&self) -> String {
        format!(
            "Client(host='{}', port={}, db='{}', user='{}')",
            self.host, self.port, self.db, self.user
        )
    }
}

#[gen_stub_pyfunction]
#[pyfunction]
#[pyo3(signature = (dsn=None, host=None, user=None, password=None, port=5432, db="postgres".to_string()))]
pub fn connect(
    dsn: Option<String>,
    host: Option<String>,
    user: Option<String>,
    password: Option<String>,
    port: u16,
    db: String,
) -> PyResult<Client> {
    if dsn.is_some() && (host.is_some() || user.is_some() || password.is_some()) {
        return Err(OxpgError::InvalidParameter(
            "Cannot specify both DSN and individual connection parameters".to_string(),
        )
        .into());
    }

    let (host, user, port, db, connection_string) = match dsn {
        Some(s) => extract_host_from_dsn(s)?,
        None => {
            let host = host.ok_or_else(|| OxpgError::MissingParameter("host".to_string()))?;
            let user = user.ok_or_else(|| OxpgError::MissingParameter("user".to_string()))?;
            let password =
                password.ok_or_else(|| OxpgError::MissingParameter("password".to_string()))?;

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
        PyErr::from(OxpgError::ConnectionFailed(format!(
            "Failed to connect to Tokio runtime: {}",
            e
        )))
    })?;

    let (client, connection) = runtime
        .block_on(async {
            tokio_postgres::connect(&connection_string, tokio_postgres::NoTls).await
        })
        .map_err(|e| {
            PyErr::from(OxpgError::ConnectionFailed(format!(
                "Failed to connect to PostgreSQL: {}",
                e
            )))
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
            PyErr::from(OxpgError::InvalidParameter(
                "Invalid DSN: must start with postgres:// or postgresql://".to_string(),
            ))
        })?;

    let (auth, rest) = without_scheme.split_once('@').ok_or_else(|| {
        PyErr::from(OxpgError::InvalidParameter(
            "Invalid DSN: missing @ separating auth and host".to_string(),
        ))
    })?;

    let (user, _) = auth.split_once(':').ok_or_else(|| {
        PyErr::from(OxpgError::InvalidParameter(
            "Invalid DSN: auth must be user:password".to_string(),
        ))
    })?;

    let (host_port, db) = rest.split_once('/').ok_or_else(|| {
        PyErr::from(OxpgError::InvalidParameter(
            "Invalid DSN: missing database name after /".to_string(),
        ))
    })?;

    let (host, port) = match host_port.split_once(':') {
        Some((h, p)) => {
            let port = p.parse::<u16>().map_err(|_| {
                PyErr::from(OxpgError::InvalidParameter(
                    "Invalid DSN: port must be a number".to_string(),
                ))
            })?;
            (h.to_string(), port)
        }
        None => (host_port.to_string(), 5432),
    };

    Ok((host, user.to_string(), port, db.to_string(), dsn))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_host_from_dsn_valid_with_port() {
        let dsn = "postgresql://user:pass@localhost:5432/mydb".to_string();
        let result = extract_host_from_dsn(dsn);
        assert!(result.is_ok());
        let (host, user, port, db, _) = result.unwrap();
        assert_eq!(host, "localhost");
        assert_eq!(user, "user");
        assert_eq!(port, 5432);
        assert_eq!(db, "mydb");
    }

    #[test]
    fn test_extract_host_from_dsn_default_port() {
        let dsn = "postgresql://user:pass@localhost/mydb".to_string();
        let result = extract_host_from_dsn(dsn);
        assert!(result.is_ok());
        let (_, _, port, _, _) = result.unwrap();
        assert_eq!(port, 5432); // Should default to 5432
    }

    #[test]
    fn test_extract_host_from_dsn_postgres_scheme() {
        let dsn = "postgres://user:pass@localhost/mydb".to_string();
        let result = extract_host_from_dsn(dsn);
        assert!(result.is_ok());
    }

    #[test]
    fn test_extract_host_from_dsn_invalid_scheme() {
        let dsn = "mysql://user:pass@localhost/db".to_string();
        let result = extract_host_from_dsn(dsn);
        assert!(result.is_err());
    }

    #[test]
    fn test_extract_host_from_dsn_missing_at_symbol() {
        let dsn = "postgresql://localhost/db".to_string();
        let result = extract_host_from_dsn(dsn);
        assert!(result.is_err());
    }

    #[test]
    fn test_extract_host_from_dsn_missing_password() {
        let dsn = "postgresql://user@localhost/db".to_string();
        let result = extract_host_from_dsn(dsn);
        assert!(result.is_err());
    }

    #[test]
    fn test_extract_host_from_dsn_missing_database() {
        let dsn = "postgresql://user:pass@localhost".to_string();
        let result = extract_host_from_dsn(dsn);
        assert!(result.is_err());
    }

    #[test]
    fn test_extract_host_from_dsn_invalid_port() {
        let dsn = "postgresql://user:pass@localhost:abc/db".to_string();
        let result = extract_host_from_dsn(dsn);
        assert!(result.is_err());
    }
}
