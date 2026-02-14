mod config;
mod conversions;

#[cfg(test)]
mod tests;

use std::sync::Arc;

use crate::client::config::validate_connect_params;
use crate::client::conversions::{extract_params, refine_params};
use crate::errors::OxpgError;
use pyo3::prelude::*;
use pyo3::types::{PyList, PyTuple};
use pyo3_async_runtimes::tokio::future_into_py;
use pyo3_stub_gen::derive::*;
use tokio_postgres::types::ToSql;
use tokio_postgres::{Client as PgClient, Config};

#[gen_stub_pyclass]
#[pyclass]
#[derive(Debug)]
pub struct Client {
    host: String,
    port: u16,
    db: String,
    user: String,
    client: Arc<PgClient>,
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
    ) -> PyResult<Bound<'a, PyAny>> {
        let client = self.client.clone();
        let mut owned_params = extract_params(args)?;
        let query = query.clone();
        future_into_py(py, async move {
            let statement = client.prepare(&query).await.map_err(|e| {
                PyErr::from(OxpgError::ExecutionError(format!(
                    "Error while generating statement: {:?}",
                    e
                )))
            })?;

            refine_params(&mut owned_params, &statement);
            let ref_params: Vec<&(dyn ToSql + Sync)> =
                owned_params.iter().map(|p| p.as_ref()).collect();

            let rows = client.query(&statement, &ref_params).await.map_err(|e| {
                PyErr::from(OxpgError::ExecutionError(format!(
                    "Error while executing query: {:?}",
                    e
                )))
            })?;

            Python::attach(|py| -> PyResult<Py<PyAny>> {
                let result = PyList::empty(py);
                for row in rows {
                    let py_row = conversions::row_to_dict(py, &row)?;
                    result.append(py_row).map_err(|e| {
                        PyErr::from(OxpgError::DataConversionError(format!(
                            "Failed to append row to result list: {:?}",
                            e
                        )))
                    })?;
                }
                Ok(result.into_any().unbind())
            })
        })
    }

    #[pyo3(signature = (query, *args))]
    fn execute<'a>(
        &'a self,
        py: Python<'a>,
        query: String,
        args: &Bound<'a, PyTuple>,
    ) -> PyResult<Bound<'a, PyAny>> {
        let client = self.client.clone();
        let mut owned_params = extract_params(args)?;
        let query = query.clone();
        future_into_py(py, async move {
            let statement = client.prepare(&query).await.map_err(|e| {
                PyErr::from(OxpgError::ExecutionError(format!(
                    "Error while generating statement: {:?}",
                    e
                )))
            })?;

            refine_params(&mut owned_params, &statement);
            let ref_params: Vec<&(dyn ToSql + Sync)> =
                owned_params.iter().map(|p| p.as_ref()).collect();

            let result = client.execute(&statement, &ref_params).await.map_err(|e| {
                PyErr::from(OxpgError::ExecutionError(format!(
                    "Error while executing query: {:?}",
                    e
                )))
            })?;

            Ok(result)
        })
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
    let client = Arc::new(client);

    Ok(Client {
        host,
        port,
        db,
        user,
        client,
        runtime,
    })
}
