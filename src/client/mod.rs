mod config;
mod conversions;

#[cfg(test)]
mod tests;

use crate::client::config::validate_connect_params;
use crate::errors::OxpgError;
use pyo3::prelude::*;
use pyo3::types::{PyList, PyTuple};
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

#[gen_stub_pymethods]
#[pymethods]
impl Client {
    #[pyo3(signature = (query, *args))]
    fn query<'a>(
        &'a self,
        py: Python<'a>,
        query: String,
        args: &Bound<'a, PyTuple>,
    ) -> PyResult<Bound<'a, PyList>> {
        let statement = py
            .detach(|| {
                self.runtime
                    .block_on(async { self.client.prepare(&query).await })
            })
            .map_err(|e| {
                PyErr::from(OxpgError::ExecutionError(format!(
                    "Prepare failed: {:?}",
                    e
                )))
            })?;

        let params = conversions::prepare_params(&statement, args)?;

        let referenced_params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
            params.iter().map(|p| p.as_ref()).collect();

        let rows = py
            .detach(|| {
                self.runtime
                    .block_on(async { self.client.query(&statement, &referenced_params).await })
            })
            .map_err(|e| {
                PyErr::from(OxpgError::ExecutionError(format!(
                    "Query execution failed: {:?}",
                    e
                )))
            })?;

        let result = PyList::empty(py);
        for row in rows {
            let row_dict = conversions::row_to_dict(py, &row)?;
            result.append(row_dict).map_err(|e| {
                PyErr::from(OxpgError::DataConversionError(format!(
                    "Failed to append row to result list: {:?}",
                    e
                )))
            })?;
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
    py: Python<'_>,
    dsn: Option<String>,
    host: Option<String>,
    user: Option<String>,
    password: Option<String>,
    port: u16,
    db: String,
) -> PyResult<Client> {
    validate_connect_params(&dsn, &host, &user, &password)?;

    let mut config = Config::new();

    let (host, user, port, db, config) = match dsn {
        Some(s) => config::extract_host_from_dsn(s, &mut config)?,
        None => {
            let host = host.ok_or_else(|| OxpgError::MissingParameter("host".to_string()))?;
            let user = user.ok_or_else(|| OxpgError::MissingParameter("user".to_string()))?;
            let password =
                password.ok_or_else(|| OxpgError::MissingParameter("password".to_string()))?;

            let config = config::populate_config_from_params(
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
        PyErr::from(OxpgError::RuntimeFailed(format!(
            "Failed to create Tokio runtime: {:?}",
            e
        )))
    })?;

    let (client, connection) = py
        .detach(|| runtime.block_on(async { config.connect(tokio_postgres::NoTls).await }))
        .map_err(|e| {
            PyErr::from(OxpgError::ConnectionFailed(format!(
                "Failed to connect to PostgreSQL: {:?}",
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
