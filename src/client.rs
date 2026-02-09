use crate::errors::OxpgError;
use chrono::{DateTime, NaiveDate, Utc};
use pyo3::types::{PyDict, PyDictMethods, PyList, PyListMethods};
use pyo3::{Bound, IntoPyObject, PyErr, PyResult, Python, pyclass, pyfunction, pymethods};
use pyo3_stub_gen::derive::*;
use tokio_postgres::types::Type;
use tokio_postgres::{Client as PgClient, Config};

#[gen_stub_pyclass]
#[pyclass]
#[derive(Debug)]
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
    fn query<'a>(&'a self, py: Python<'a>, query: String) -> PyResult<Bound<'a, PyList>> {
        let rows = self
            .runtime
            .block_on(async { self.client.query(&query, &[]).await })
            .map_err(|e| {
                PyErr::from(OxpgError::QueryFailed(format!(
                    "Failed to execute query: {}",
                    e
                )))
            })?;

        let result = PyList::empty(py);
        for row in rows {
            let row_dict = PyDict::new(py);
            for (idx, column) in row.columns().iter().enumerate() {
                let value = match *column.type_() {
                    Type::BOOL => row.get::<_, Option<bool>>(idx).into_pyobject(py)?,
                    Type::BYTEA => row.get::<_, Option<Vec<u8>>>(idx).into_pyobject(py)?,
                    Type::DATE => row
                        .get::<_, Option<NaiveDate>>(idx)
                        .map(|d| d.to_string())
                        .into_pyobject(py)?,
                    Type::INT2 => row.get::<_, Option<i16>>(idx).into_pyobject(py)?,
                    Type::INT4 => row.get::<_, Option<i32>>(idx).into_pyobject(py)?,
                    Type::INT8 => row.get::<_, Option<i64>>(idx).into_pyobject(py)?,
                    Type::JSON | Type::JSONB => row
                        .get::<_, Option<serde_json::Value>>(idx)
                        .map(|v| v.to_string())
                        .into_pyobject(py)?,
                    Type::NUMERIC => row.get::<_, Option<String>>(idx).into_pyobject(py)?,
                    Type::FLOAT4 => row.get::<_, Option<f32>>(idx).into_pyobject(py)?,
                    Type::FLOAT8 => row.get::<_, Option<f64>>(idx).into_pyobject(py)?,
                    Type::TEXT | Type::VARCHAR | Type::BPCHAR => {
                        row.get::<_, Option<String>>(idx).into_pyobject(py)?
                    }
                    Type::TIME => row
                        .get::<_, Option<chrono::NaiveTime>>(idx)
                        .map(|t| t.to_string())
                        .into_pyobject(py)?,
                    Type::TIMESTAMP => row
                        .get::<_, Option<chrono::NaiveDateTime>>(idx)
                        .map(|dt| dt.to_string())
                        .into_pyobject(py)?,
                    Type::TIMESTAMPTZ => row
                        .get::<_, Option<DateTime<Utc>>>(idx)
                        .map(|dt| dt.to_string())
                        .into_pyobject(py)?,
                    Type::UUID => row
                        .get::<_, Option<uuid::Uuid>>(idx)
                        .map(|u| u.to_string())
                        .into_pyobject(py)?,
                    _ => py.None().into_pyobject(py)?,
                };

                row_dict.set_item(column.name(), value)?;
            }
            result.append(row_dict)?;
        }
        Ok(result)
    }

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

    let mut config = Config::new();

    let (host, user, port, db, config) = match dsn {
        Some(s) => extract_host_from_dsn(s, &mut config)?,
        None => {
            let host = host.ok_or_else(|| OxpgError::MissingParameter("host".to_string()))?;
            let user = user.ok_or_else(|| OxpgError::MissingParameter("user".to_string()))?;
            let password =
                password.ok_or_else(|| OxpgError::MissingParameter("password".to_string()))?;
            let config = populate_config_from_params(
                host.clone(),
                user.clone(),
                password,
                port,
                db.clone(),
                &mut config,
            );
            (host, user, port, db, config)
        }
    };

    let runtime = tokio::runtime::Runtime::new().map_err(|e| {
        PyErr::from(OxpgError::ConnectionFailed(format!(
            "Failed to connect to Tokio runtime: {}",
            e
        )))
    })?;

    let (client, connection) = runtime
        .block_on(async { config.connect(tokio_postgres::NoTls).await })
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

fn extract_host_from_dsn(
    dsn: String,
    config: &mut Config,
) -> PyResult<(String, String, u16, String, &mut Config)> {
    let parsed_config: Config = dsn
        .parse()
        .map_err(|e| PyErr::from(OxpgError::InvalidParameter(format!("Invalid DSN: {}", e))))?;

    let host = parsed_config
        .get_hosts()
        .first()
        .and_then(|h| match h {
            tokio_postgres::config::Host::Tcp(s) => Some(s.clone()),
            _ => None,
        })
        .ok_or_else(|| OxpgError::InvalidParameter("No host in DSN".to_string()))?;

    let user = parsed_config
        .get_user()
        .ok_or_else(|| OxpgError::MissingParameter("user".to_string()))?
        .to_string();

    let port = parsed_config.get_ports().first().copied().unwrap_or(5432);

    let db = parsed_config
        .get_dbname()
        .ok_or_else(|| OxpgError::MissingParameter("database".to_string()))?
        .to_string();

    *config = parsed_config;

    Ok((host, user, port, db, config))
}

fn populate_config_from_params(
    host: String,
    user: String,
    password: String,
    port: u16,
    db: String,
    config: &mut Config,
) -> &mut Config {
    config
        .host(&host)
        .port(port)
        .user(&user)
        .password(&password)
        .dbname(&db)
}

#[cfg(test)]
mod tests {
    use super::*;

    mod populate_config_from_params {
        use super::*;

        #[test]
        fn returns_same_config_reference() {
            let mut config = Config::new();

            let result = populate_config_from_params(
                "testhost".to_string(),
                "testuser".to_string(),
                "testpass".to_string(),
                5433,
                "testdb".to_string(),
                &mut config,
            );

            assert!(std::ptr::eq(result, &config));
        }

        #[test]
        fn does_not_panic_with_special_characters() {
            let mut config = Config::new();

            populate_config_from_params(
                "host-with-dashes".to_string(),
                "user@domain".to_string(),
                "p@ss:w0rd!".to_string(),
                5432,
                "db-name_123".to_string(),
                &mut config,
            );
        }
    }

    mod extract_host_from_dsn {
        use super::*;

        #[test]
        fn extracts_all_components_from_valid_dsn() {
            let dsn = "postgresql://myuser:mypass@dbhost:5433/mydb".to_string();
            let mut config = Config::new();

            let result = extract_host_from_dsn(dsn, &mut config);

            assert!(result.is_ok());

            if let Ok((host, user, port, db, _)) = result {
                assert_eq!(host, "dbhost");
                assert_eq!(user, "myuser");
                assert_eq!(port, 5433);
                assert_eq!(db, "mydb");
            }
        }

        #[test]
        fn uses_default_port_when_not_specified() {
            let dsn = "postgresql://user:pass@localhost/mydb".to_string();
            let mut config = Config::new();

            if let Ok((_, _, port, _, _)) = extract_host_from_dsn(dsn, &mut config) {
                assert_eq!(port, 5432);
            }
        }

        #[test]
        fn handles_ipv4_addresses() {
            let dsn = "postgresql://user:pass@192.168.1.1:5432/mydb".to_string();
            let mut config = Config::new();

            if let Ok((host, _, _, _, _)) = extract_host_from_dsn(dsn, &mut config) {
                assert_eq!(host, "192.168.1.1");
            }
        }

        #[test]
        fn handles_percent_encoded_credentials() {
            let dsn = "postgresql://user:p%40ss%3Aword@localhost/mydb".to_string();
            let mut config = Config::new();

            assert!(extract_host_from_dsn(dsn, &mut config).is_ok());
        }

        #[test]
        fn rejects_invalid_scheme() {
            let dsn = "mysql://user:pass@localhost/db".to_string();
            let mut config = Config::new();

            assert!(extract_host_from_dsn(dsn, &mut config).is_err());
        }

        #[test]
        fn rejects_missing_user() {
            let dsn = "postgresql://localhost/db".to_string();
            let mut config = Config::new();

            assert!(extract_host_from_dsn(dsn, &mut config).is_err());
        }

        #[test]
        fn rejects_missing_database() {
            let dsn = "postgresql://user:pass@localhost".to_string();
            let mut config = Config::new();

            assert!(extract_host_from_dsn(dsn, &mut config).is_err());
        }
    }
}
