use crate::errors::OxpgError;
use pyo3::{PyErr, PyResult, pyclass, pyfunction, pymethods};
use pyo3_stub_gen::derive::*;
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

#[gen_stub_pyclass]
#[pyclass]
#[derive(Debug)]
pub struct Row {}

#[gen_stub_pymethods]
#[pymethods]
impl Client {
    fn query(&self, query: String) -> PyResult<()> {
        let rows = self
            .runtime
            .block_on(async { self.client.query(&query, &[]).await })
            .map_err(|e| {
                PyErr::from(OxpgError::QueryFailed(format!(
                    "Failed to execute query: {}",
                    e
                )))
            })?;
        println!("Query returned: {:?}", rows);
        Ok(())
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

    // Extract values for the Client struct fields
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

    mod extract_host_from_dsn {
        use super::*;

        #[test]
        fn valid_dsn_with_explicit_port() {
            let dsn = "postgresql://user:pass@localhost:5432/mydb".to_string();
            let mut config = Config::new();

            let (host, user, port, db, _) =
                extract_host_from_dsn(dsn, &mut config).expect("should parse valid DSN");

            assert_eq!(host, "localhost");
            assert_eq!(user, "user");
            assert_eq!(port, 5432);
            assert_eq!(db, "mydb");
        }

        #[test]
        fn valid_dsn_without_port_uses_default() {
            let dsn = "postgresql://user:pass@localhost/mydb".to_string();
            let mut config = Config::new();

            let (_, _, port, _, _) =
                extract_host_from_dsn(dsn, &mut config).expect("should parse DSN without port");

            assert_eq!(port, 5432, "should default to PostgreSQL standard port");
        }

        #[test]
        fn accepts_postgres_scheme_variant() {
            let dsn = "postgres://user:pass@localhost/mydb".to_string();
            let mut config = Config::new();

            let result = extract_host_from_dsn(dsn, &mut config);

            assert!(result.is_ok(), "should accept 'postgres://' scheme");
        }

        #[test]
        fn rejects_invalid_scheme() {
            let dsn = "mysql://user:pass@localhost/db".to_string();
            let mut config = Config::new();

            let err = extract_host_from_dsn(dsn, &mut config)
                .expect_err("should reject non-PostgreSQL schemes");

            let err_msg = err.to_string();
            assert!(
                err_msg.contains("must start with postgres://"),
                "error message should mention valid schemes, got: {err_msg}"
            );
        }

        #[test]
        fn rejects_missing_auth_separator() {
            let dsn = "postgresql://localhost/db".to_string();
            let mut config = Config::new();

            let err = extract_host_from_dsn(dsn, &mut config)
                .expect_err("should reject DSN without @ separator");

            let err_msg = err.to_string();
            assert!(
                err_msg.contains("missing @ separating"),
                "error message should mention missing @, got: {err_msg}"
            );
        }

        #[test]
        fn rejects_missing_password() {
            let dsn = "postgresql://user@localhost/db".to_string();
            let mut config = Config::new();

            let err = extract_host_from_dsn(dsn, &mut config)
                .expect_err("should reject DSN without password");

            let err_msg = err.to_string();
            assert!(
                err_msg.contains("user:password"),
                "error message should mention required format, got: {err_msg}"
            );
        }

        #[test]
        fn rejects_missing_database() {
            let dsn = "postgresql://user:pass@localhost".to_string();
            let mut config = Config::new();

            let err = extract_host_from_dsn(dsn, &mut config)
                .expect_err("should reject DSN without database name");

            let err_msg = err.to_string();
            assert!(
                err_msg.contains("missing database name"),
                "error message should mention missing database, got: {err_msg}"
            );
        }

        #[test]
        fn rejects_invalid_port() {
            let dsn = "postgresql://user:pass@localhost:abc/db".to_string();
            let mut config = Config::new();

            let err = extract_host_from_dsn(dsn, &mut config)
                .expect_err("should reject non-numeric port");

            let err_msg = err.to_string();
            assert!(
                err_msg.contains("port must be a number"),
                "error message should mention port validation, got: {err_msg}"
            );
        }

        #[test]
        fn handles_ipv4_address() {
            let dsn = "postgresql://user:pass@192.168.1.1:5432/mydb".to_string();
            let mut config = Config::new();

            let (host, _, _, _, _) =
                extract_host_from_dsn(dsn, &mut config).expect("should parse IPv4 addresses");

            assert_eq!(host, "192.168.1.1");
        }

        #[test]
        fn handles_non_standard_port() {
            let dsn = "postgresql://user:pass@localhost:9999/mydb".to_string();
            let mut config = Config::new();

            let (_, _, port, _, _) =
                extract_host_from_dsn(dsn, &mut config).expect("should parse non-standard ports");

            assert_eq!(port, 9999);
        }
    }

    mod populate_config_from_params {
        use super::*;

        #[test]
        fn populates_all_connection_parameters() {
            let mut config = Config::new();

            let result = populate_config_from_params(
                "testhost".to_string(),
                "testuser".to_string(),
                "testpass".to_string(),
                5433,
                "testdb".to_string(),
                &mut config,
            );

            // Verify it returns the config (chaining works)
            assert!(
                std::ptr::eq(result, &config),
                "should return mutable reference to same config"
            );
        }

        #[test]
        fn handles_special_characters_in_credentials() {
            let mut config = Config::new();

            // Should not panic with special characters
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

    mod connect_function {
        use super::*;

        #[test]
        fn rejects_both_dsn_and_individual_params() {
            let result = connect(
                Some("postgresql://user:pass@localhost/db".to_string()),
                Some("localhost".to_string()),
                None,
                None,
                5432,
                "postgres".to_string(),
            );

            let err = result.expect_err("should reject DSN + individual params");
            let err_msg = err.to_string();
            assert!(
                err_msg.contains("Cannot specify both DSN"),
                "error should mention parameter conflict, got: {err_msg}"
            );
        }

        #[test]
        fn requires_host_when_no_dsn() {
            let result = connect(
                None,
                None,
                Some("user".to_string()),
                Some("pass".to_string()),
                5432,
                "postgres".to_string(),
            );

            let err = result.expect_err("should require host parameter");
            let err_msg = err.to_string();
            assert!(
                err_msg.contains("host"),
                "error should mention missing host, got: {err_msg}"
            );
        }

        #[test]
        fn requires_user_when_no_dsn() {
            let result = connect(
                None,
                Some("localhost".to_string()),
                None,
                Some("pass".to_string()),
                5432,
                "postgres".to_string(),
            );

            let err = result.expect_err("should require user parameter");
            let err_msg = err.to_string();
            assert!(
                err_msg.contains("user"),
                "error should mention missing user, got: {err_msg}"
            );
        }

        #[test]
        fn requires_password_when_no_dsn() {
            let result = connect(
                None,
                Some("localhost".to_string()),
                Some("user".to_string()),
                None,
                5432,
                "postgres".to_string(),
            );

            let err = result.expect_err("should require password parameter");
            let err_msg = err.to_string();
            assert!(
                err_msg.contains("password"),
                "error should mention missing password, got: {err_msg}"
            );
        }
    }
}
