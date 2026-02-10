use crate::errors::OxpgError;
use pyo3::{PyErr, PyResult};
use tokio_postgres::Config;

pub(crate) fn extract_host_from_dsn(
    dsn: String,
    config: &mut Config,
) -> PyResult<(String, String, u16, String, &mut Config)> {
    let parsed_config: Config = dsn
        .parse()
        .map_err(|e| PyErr::from(OxpgError::InvalidDsn(format!("{}", e))))?;

    let host = parsed_config
        .get_hosts()
        .first()
        .and_then(|h| match h {
            tokio_postgres::config::Host::Tcp(s) => Some(s.clone()),
            _ => None,
        })
        .ok_or_else(|| OxpgError::MissingParameter("host".to_string()))?;

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

pub(crate) fn populate_config_from_params(
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

pub(crate) fn validate_connect_params(
    dsn: &Option<String>,
    host: &Option<String>,
    user: &Option<String>,
    password: &Option<String>,
) -> Result<(), OxpgError> {
    if dsn.is_some() && (host.is_some() || user.is_some() || password.is_some()) {
        return Err(OxpgError::InvalidParameter(
            "Cannot specify both DSN and individual connection parameters".to_string(),
        ));
    }

    if dsn.is_none() {
        if host.is_none() {
            return Err(OxpgError::MissingParameter("host".to_string()));
        }
        if user.is_none() {
            return Err(OxpgError::MissingParameter("user".to_string()));
        }
        if password.is_none() {
            return Err(OxpgError::MissingParameter("password".to_string()));
        }
    }

    Ok(())
}
