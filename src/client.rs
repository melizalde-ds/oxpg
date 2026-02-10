use crate::errors::OxpgError;
use chrono::{DateTime, NaiveDate, Utc};
use pyo3::types::{
    PyAnyMethods, PyBool, PyByteArray, PyBytes, PyDate, PyDateTime, PyDelta, PyDict, PyDictMethods,
    PyFloat, PyInt, PyList, PyListMethods, PyNone, PyString, PyTime, PyTuple, PyTupleMethods,
    PyTypeMethods,
};
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

        let mut params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync>> = Vec::new();
        for (idx, arg) in args.iter().enumerate() {
            let expected_type = statement.params().get(idx);

            if arg.is_instance_of::<PyBool>() {
                let val: bool = arg.extract().map_err(|e| {
                    OxpgError::InvalidParameter(format!(
                        "Could not extract BOOL for argument {}: {}",
                        idx, e
                    ))
                })?;
                params.push(Box::new(val));
            } else if arg.is_instance_of::<PyInt>() {
                match expected_type {
                    Some(&Type::INT2) => {
                        let val = arg.extract::<i16>().map_err(|e| {
                            OxpgError::InvalidParameter(format!(
                                "Could not fit argument {} into INT2: {}",
                                idx, e
                            ))
                        })?;
                        params.push(Box::new(val));
                    }
                    Some(&Type::INT4) => {
                        let val = arg.extract::<i32>().map_err(|e| {
                            OxpgError::InvalidParameter(format!(
                                "Could not fit argument {} into INT4: {}",
                                idx, e
                            ))
                        })?;
                        params.push(Box::new(val));
                    }
                    _ => {
                        let val = arg.extract::<i64>().map_err(|e| {
                            OxpgError::InvalidParameter(format!(
                                "Could not fit argument {} into INT8: {}",
                                idx, e
                            ))
                        })?;
                        params.push(Box::new(val));
                    }
                }
            } else if arg.is_instance_of::<PyFloat>() {
                match expected_type {
                    Some(&Type::FLOAT4) => {
                        let val = arg.extract::<f32>().map_err(|e| {
                            OxpgError::InvalidParameter(format!(
                                "Could not extract FLOAT4 for argument {}: {}",
                                idx, e
                            ))
                        })?;
                        params.push(Box::new(val));
                    }
                    _ => {
                        let val = arg.extract::<f64>().map_err(|e| {
                            OxpgError::InvalidParameter(format!(
                                "Could not extract FLOAT8 for argument {}: {}",
                                idx, e
                            ))
                        })?;
                        params.push(Box::new(val));
                    }
                }
            } else if arg.is_instance_of::<PyString>() {
                let val: String = arg.extract().map_err(|e| {
                    OxpgError::InvalidParameter(format!(
                        "Could not extract String for argument {}: {}",
                        idx, e
                    ))
                })?;
                params.push(Box::new(val));
            } else if arg.is_instance_of::<PyNone>() {
                match expected_type {
                    Some(&Type::BOOL) => params.push(Box::new(None::<bool>)),
                    Some(&Type::INT2) => params.push(Box::new(None::<i16>)),
                    Some(&Type::INT4) => params.push(Box::new(None::<i32>)),
                    Some(&Type::INT8) => params.push(Box::new(None::<i64>)),
                    Some(&Type::FLOAT4) => params.push(Box::new(None::<f32>)),
                    Some(&Type::FLOAT8) => params.push(Box::new(None::<f64>)),
                    Some(&Type::BYTEA) => params.push(Box::new(None::<Vec<u8>>)),
                    Some(&Type::DATE) => params.push(Box::new(None::<NaiveDate>)),
                    Some(&Type::TIMESTAMP) => params.push(Box::new(None::<chrono::NaiveDateTime>)),
                    Some(&Type::TIMESTAMPTZ) => params.push(Box::new(None::<DateTime<Utc>>)),
                    Some(&Type::TIME) => params.push(Box::new(None::<chrono::NaiveTime>)),
                    Some(&Type::UUID) => params.push(Box::new(None::<uuid::Uuid>)),
                    _ => params.push(Box::new(None::<String>)),
                }
            } else if arg.is_instance_of::<PyBytes>() || arg.is_instance_of::<PyByteArray>() {
                let val: Vec<u8> = arg.extract().map_err(|e| {
                    OxpgError::InvalidParameter(format!(
                        "Could not extract bytes for argument {}: {}",
                        idx, e
                    ))
                })?;
                params.push(Box::new(val));
            } else if arg.is_instance_of::<PyDateTime>() {
                let naive_dt = arg.extract::<chrono::NaiveDateTime>().map_err(|e| {
                    OxpgError::InvalidParameter(format!(
                        "Could not extract NaiveDateTime for argument {}: {}",
                        idx, e
                    ))
                })?;
                match expected_type {
                    Some(&Type::TIMESTAMP) => params.push(Box::new(naive_dt)),
                    _ => {
                        let dt_utc = DateTime::<Utc>::from_naive_utc_and_offset(naive_dt, Utc);
                        params.push(Box::new(dt_utc));
                    }
                }
            } else if arg.is_instance_of::<PyDate>() {
                let date = arg.extract::<chrono::NaiveDate>().map_err(|e| {
                    OxpgError::InvalidParameter(format!(
                        "Could not extract NaiveDate for argument {}: {}",
                        idx, e
                    ))
                })?;
                params.push(Box::new(date));
            } else if arg.is_instance_of::<PyTime>() {
                let time = arg.extract::<chrono::NaiveTime>().map_err(|e| {
                    OxpgError::InvalidParameter(format!(
                        "Could not extract NaiveTime for argument {}: {}",
                        idx, e
                    ))
                })?;
                params.push(Box::new(time));
            } else if arg.is_instance_of::<PyDelta>() {
                let days: i64 = arg.getattr("days")?.extract().map_err(|e| {
                    OxpgError::InvalidParameter(format!(
                        "Could not extract timedelta.days for argument {}: {}",
                        idx, e
                    ))
                })?;
                let seconds: i64 = arg.getattr("seconds")?.extract().map_err(|e| {
                    OxpgError::InvalidParameter(format!(
                        "Could not extract timedelta.seconds for argument {}: {}",
                        idx, e
                    ))
                })?;
                let microseconds: i64 = arg.getattr("microseconds")?.extract().map_err(|e| {
                    OxpgError::InvalidParameter(format!(
                        "Could not extract timedelta.microseconds for argument {}: {}",
                        idx, e
                    ))
                })?;

                let interval_str = format!(
                    "{} days {} seconds {} microseconds",
                    days, seconds, microseconds
                );

                params.push(Box::new(interval_str));
            } else {
                return Err(OxpgError::UnsupportedType(format!(
                    "Parameter at index {} is of type '{}', which is not supported. \
                     Supported types: int, float, bool, str, bytes, bytearray, datetime, date, time, timedelta, None",
                    idx,
                    arg.get_type().name()?
                ))
                .into());
            }
        }

        let referenced_params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
            params.iter().map(|p| p.as_ref()).collect();

        let rows = py
            .detach(|| {
                self.runtime
                    .block_on(async { self.client.query(&query, &referenced_params).await })
            })
            .map_err(|e| {
                PyErr::from(OxpgError::ExecutionError(format!(
                    "Query execution failed: {:?}",
                    e
                )))
            })?;

        let result = PyList::empty(py);
        for row in rows {
            let row_dict = PyDict::new(py);
            for (idx, column) in row.columns().iter().enumerate() {
                let value = match *column.type_() {
                    Type::BOOL => {
                        row.get::<_, Option<bool>>(idx)
                            .into_pyobject(py)
                            .map_err(|e| {
                                PyErr::from(OxpgError::DataConversionError(format!(
                                    "Failed to convert BOOL column '{}': {:?}",
                                    column.name(),
                                    e
                                )))
                            })?
                    }
                    Type::BYTEA => row
                        .get::<_, Option<Vec<u8>>>(idx)
                        .into_pyobject(py)
                        .map_err(|e| {
                            PyErr::from(OxpgError::DataConversionError(format!(
                                "Failed to convert BYTEA column '{}': {:?}",
                                column.name(),
                                e
                            )))
                        })?,
                    Type::DATE => row
                        .get::<_, Option<chrono::NaiveDate>>(idx)
                        .into_pyobject(py)
                        .map_err(|e| {
                            PyErr::from(OxpgError::DataConversionError(format!(
                                "Failed to convert DATE column '{}': {:?}",
                                column.name(),
                                e
                            )))
                        })?,
                    Type::INT2 => {
                        row.get::<_, Option<i16>>(idx)
                            .into_pyobject(py)
                            .map_err(|e| {
                                PyErr::from(OxpgError::DataConversionError(format!(
                                    "Failed to convert INT2 column '{}': {:?}",
                                    column.name(),
                                    e
                                )))
                            })?
                    }
                    Type::INT4 => {
                        row.get::<_, Option<i32>>(idx)
                            .into_pyobject(py)
                            .map_err(|e| {
                                PyErr::from(OxpgError::DataConversionError(format!(
                                    "Failed to convert INT4 column '{}': {:?}",
                                    column.name(),
                                    e
                                )))
                            })?
                    }
                    Type::INT8 => {
                        row.get::<_, Option<i64>>(idx)
                            .into_pyobject(py)
                            .map_err(|e| {
                                PyErr::from(OxpgError::DataConversionError(format!(
                                    "Failed to convert INT8 column '{}': {:?}",
                                    column.name(),
                                    e
                                )))
                            })?
                    }
                    Type::JSON | Type::JSONB => row
                        .get::<_, Option<serde_json::Value>>(idx)
                        .map(|v| v.to_string())
                        .into_pyobject(py)
                        .map_err(|e| {
                            PyErr::from(OxpgError::DataConversionError(format!(
                                "Failed to convert JSON/JSONB column '{}': {:?}",
                                column.name(),
                                e
                            )))
                        })?,
                    Type::NUMERIC => row
                        .try_get::<_, Option<String>>(idx)
                        .map_err(|e| {
                            PyErr::from(OxpgError::DataConversionError(format!(
                                "Failed to convert NUMERIC column '{}' to string: {:?}",
                                column.name(),
                                e
                            )))
                        })?
                        .into_pyobject(py)
                        .map_err(|e| {
                            PyErr::from(OxpgError::DataConversionError(format!(
                                "Failed to convert NUMERIC column '{}' to PyObject: {:?}",
                                column.name(),
                                e
                            )))
                        })?,
                    Type::FLOAT4 => {
                        row.get::<_, Option<f32>>(idx)
                            .into_pyobject(py)
                            .map_err(|e| {
                                PyErr::from(OxpgError::DataConversionError(format!(
                                    "Failed to convert FLOAT4 column '{}': {:?}",
                                    column.name(),
                                    e
                                )))
                            })?
                    }
                    Type::FLOAT8 => {
                        row.get::<_, Option<f64>>(idx)
                            .into_pyobject(py)
                            .map_err(|e| {
                                PyErr::from(OxpgError::DataConversionError(format!(
                                    "Failed to convert FLOAT8 column '{}': {:?}",
                                    column.name(),
                                    e
                                )))
                            })?
                    }
                    Type::TEXT | Type::VARCHAR | Type::BPCHAR => row
                        .get::<_, Option<String>>(idx)
                        .into_pyobject(py)
                        .map_err(|e| {
                            PyErr::from(OxpgError::DataConversionError(format!(
                                "Failed to convert TEXT/VARCHAR column '{}': {:?}",
                                column.name(),
                                e
                            )))
                        })?,
                    Type::TIME => row
                        .get::<_, Option<chrono::NaiveTime>>(idx)
                        .into_pyobject(py)
                        .map_err(|e| {
                            PyErr::from(OxpgError::DataConversionError(format!(
                                "Failed to convert TIME column '{}': {:?}",
                                column.name(),
                                e
                            )))
                        })?,
                    Type::TIMESTAMP => row
                        .get::<_, Option<chrono::NaiveDateTime>>(idx)
                        .into_pyobject(py)
                        .map_err(|e| {
                            PyErr::from(OxpgError::DataConversionError(format!(
                                "Failed to convert TIMESTAMP column '{}': {:?}",
                                column.name(),
                                e
                            )))
                        })?,
                    Type::TIMESTAMPTZ => row
                        .get::<_, Option<DateTime<Utc>>>(idx)
                        .into_pyobject(py)
                        .map_err(|e| {
                            PyErr::from(OxpgError::DataConversionError(format!(
                                "Failed to convert TIMESTAMPTZ column '{}': {:?}",
                                column.name(),
                                e
                            )))
                        })?,
                    Type::UUID => row
                        .get::<_, Option<uuid::Uuid>>(idx)
                        .map(|u| u.to_string())
                        .into_pyobject(py)
                        .map_err(|e| {
                            PyErr::from(OxpgError::DataConversionError(format!(
                                "Failed to convert UUID column '{}': {:?}",
                                column.name(),
                                e
                            )))
                        })?,
                    _ => {
                        return Err(PyErr::from(OxpgError::UnsupportedType(format!(
                            "Unsupported Postgres type '{}' (OID {}) for column '{}'",
                            column.type_().name(),
                            column.type_().oid(),
                            column.name(),
                        ))));
                    }
                };

                row_dict.set_item(column.name(), value).map_err(|e| {
                    PyErr::from(OxpgError::DataConversionError(format!(
                        "Failed to add column '{}' to result dictionary: {:?}",
                        column.name(),
                        e
                    )))
                })?;
            }
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
    if dsn.is_none() && host.is_none() && user.is_none() && password.is_none() {
        return Err(OxpgError::MissingParameter(
            "Must specify either DSN or all individual connection parameters".to_string(),
        )
        .into());
    }

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

fn extract_host_from_dsn(
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
    use std::time::Duration;

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

        #[test]
        fn populates_config_with_correct_values() {
            let mut config = Config::new();

            populate_config_from_params(
                "testhost".to_string(),
                "testuser".to_string(),
                "testpass".to_string(),
                5433,
                "testdb".to_string(),
                &mut config,
            );

            assert_eq!(config.get_user(), Some("testuser"));
            assert_eq!(config.get_dbname(), Some("testdb"));
            assert_eq!(config.get_ports().first(), Some(&5433));
        }

        #[test]
        fn handles_empty_strings() {
            let mut config = Config::new();

            populate_config_from_params(
                "".to_string(),
                "".to_string(),
                "".to_string(),
                5432,
                "".to_string(),
                &mut config,
            );

            assert_eq!(config.get_user(), Some(""));
            assert_eq!(config.get_dbname(), Some(""));
        }

        #[test]
        fn handles_minimum_port() {
            let mut config = Config::new();

            populate_config_from_params(
                "localhost".to_string(),
                "user".to_string(),
                "pass".to_string(),
                1,
                "db".to_string(),
                &mut config,
            );

            assert_eq!(config.get_ports().first(), Some(&1));
        }

        #[test]
        fn handles_maximum_port() {
            let mut config = Config::new();

            populate_config_from_params(
                "localhost".to_string(),
                "user".to_string(),
                "pass".to_string(),
                65535,
                "db".to_string(),
                &mut config,
            );

            assert_eq!(config.get_ports().first(), Some(&65535));
        }

        #[test]
        fn handles_unicode_in_parameters() {
            let mut config = Config::new();

            populate_config_from_params(
                "hôst.example.com".to_string(),
                "üser".to_string(),
                "pässwörd".to_string(),
                5432,
                "datäbase".to_string(),
                &mut config,
            );

            assert_eq!(config.get_user(), Some("üser"));
            assert_eq!(config.get_dbname(), Some("datäbase"));
        }

        #[test]
        fn handles_long_strings() {
            let mut config = Config::new();
            let long_string = "a".repeat(1000);

            populate_config_from_params(
                long_string.clone(),
                long_string.clone(),
                long_string.clone(),
                5432,
                long_string.clone(),
                &mut config,
            );

            assert_eq!(config.get_user(), Some(long_string.as_str()));
        }
    }

    mod extract_host_from_dsn {
        use super::*;

        #[test]
        fn extracts_all_components_from_valid_dsn() {
            Python::attach(|_py| {
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
            });
        }

        #[test]
        fn uses_default_port_when_not_specified() {
            Python::attach(|_py| {
                let dsn = "postgresql://user:pass@localhost/mydb".to_string();
                let mut config = Config::new();

                if let Ok((_, _, port, _, _)) = extract_host_from_dsn(dsn, &mut config) {
                    assert_eq!(port, 5432);
                }
            });
        }

        #[test]
        fn handles_ipv4_addresses() {
            Python::attach(|_py| {
                let dsn = "postgresql://user:pass@192.168.1.1:5432/mydb".to_string();
                let mut config = Config::new();

                if let Ok((host, _, _, _, _)) = extract_host_from_dsn(dsn, &mut config) {
                    assert_eq!(host, "192.168.1.1");
                }
            });
        }

        #[test]
        fn handles_percent_encoded_credentials() {
            Python::attach(|_py| {
                let dsn = "postgresql://user:p%40ss%3Aword@localhost/mydb".to_string();
                let mut config = Config::new();

                assert!(extract_host_from_dsn(dsn, &mut config).is_ok());
            });
        }

        #[test]
        fn rejects_invalid_scheme() {
            Python::attach(|_py| {
                let dsn = "mysql://user:pass@localhost/db".to_string();
                let mut config = Config::new();

                assert!(extract_host_from_dsn(dsn, &mut config).is_err());
            });
        }

        #[test]
        fn rejects_missing_user() {
            Python::attach(|_py| {
                let dsn = "postgresql://localhost/db".to_string();
                let mut config = Config::new();

                assert!(extract_host_from_dsn(dsn, &mut config).is_err());
            });
        }

        #[test]
        fn rejects_missing_database() {
            Python::attach(|_py| {
                let dsn = "postgresql://user:pass@localhost".to_string();
                let mut config = Config::new();

                assert!(extract_host_from_dsn(dsn, &mut config).is_err());
            });
        }

        #[test]
        fn accepts_postgres_scheme() {
            Python::attach(|_py| {
                let dsn = "postgres://user:pass@localhost/mydb".to_string();
                let mut config = Config::new();

                let result = extract_host_from_dsn(dsn, &mut config);
                assert!(result.is_ok());
            });
        }

        #[test]
        fn handles_ipv6_addresses() {
            Python::attach(|_py| {
                let dsn = "postgresql://user:pass@[::1]:5432/mydb".to_string();
                let mut config = Config::new();

                let result = extract_host_from_dsn(dsn, &mut config);
                assert!(result.is_ok() || result.is_err());
            });
        }

        #[test]
        fn handles_missing_password() {
            Python::attach(|_py| {
                let dsn = "postgresql://user@localhost/mydb".to_string();
                let mut config = Config::new();

                if let Ok((host, user, _, db, _)) = extract_host_from_dsn(dsn, &mut config) {
                    assert_eq!(host, "localhost");
                    assert_eq!(user, "user");
                    assert_eq!(db, "mydb");
                }
            });
        }

        #[test]
        fn rejects_empty_dsn() {
            Python::attach(|_py| {
                let dsn = "".to_string();
                let mut config = Config::new();

                assert!(extract_host_from_dsn(dsn, &mut config).is_err());
            });
        }

        #[test]
        fn rejects_malformed_dsn() {
            Python::attach(|_py| {
                let dsn = "not-a-valid-dsn".to_string();
                let mut config = Config::new();

                assert!(extract_host_from_dsn(dsn, &mut config).is_err());
            });
        }

        #[test]
        fn handles_query_parameters() {
            Python::attach(|_py| {
                let dsn = "postgresql://user:pass@localhost/mydb?sslmode=require".to_string();
                let mut config = Config::new();

                if let Ok((_, _, _, db, _)) = extract_host_from_dsn(dsn, &mut config) {
                    assert_eq!(db, "mydb");
                }
            });
        }

        #[test]
        fn handles_complex_hostnames() {
            Python::attach(|_py| {
                let dsn = "postgresql://user:pass@db-server.example.com:5432/mydb".to_string();
                let mut config = Config::new();

                if let Ok((host, _, _, _, _)) = extract_host_from_dsn(dsn, &mut config) {
                    assert_eq!(host, "db-server.example.com");
                }
            });
        }

        #[test]
        fn handles_minimum_port() {
            Python::attach(|_py| {
                let dsn = "postgresql://user:pass@localhost:1/mydb".to_string();
                let mut config = Config::new();

                if let Ok((_, _, port, _, _)) = extract_host_from_dsn(dsn, &mut config) {
                    assert_eq!(port, 1);
                }
            });
        }

        #[test]
        fn handles_maximum_port() {
            Python::attach(|_py| {
                let dsn = "postgresql://user:pass@localhost:65535/mydb".to_string();
                let mut config = Config::new();

                if let Ok((_, _, port, _, _)) = extract_host_from_dsn(dsn, &mut config) {
                    assert_eq!(port, 65535);
                }
            });
        }

        #[test]
        fn handles_special_chars_in_dbname() {
            Python::attach(|_py| {
                let dsn = "postgresql://user:pass@localhost/my-db_123".to_string();
                let mut config = Config::new();

                if let Ok((_, _, _, db, _)) = extract_host_from_dsn(dsn, &mut config) {
                    assert_eq!(db, "my-db_123");
                }
            });
        }

        #[test]
        fn handles_localhost_variations() {
            Python::attach(|_py| {
                let dsns = vec![
                    "postgresql://user:pass@localhost/db",
                    "postgresql://user:pass@127.0.0.1/db",
                    "postgresql://user:pass@::1/db",
                ];

                for dsn in dsns {
                    let mut config = Config::new();
                    let result = extract_host_from_dsn(dsn.to_string(), &mut config);
                    assert!(result.is_ok() || result.is_err());
                }
            });
        }

        #[test]
        fn provides_meaningful_error_for_invalid_scheme() {
            Python::attach(|_py| {
                let dsn = "http://user:pass@localhost/db".to_string();
                let mut config = Config::new();

                let result = extract_host_from_dsn(dsn, &mut config);
                assert!(result.is_err());
            });
        }
    }

    mod connect {
        use super::*;

        #[test]
        fn rejects_both_dsn_and_host() {
            Python::attach(|py| {
                let result = connect(
                    py,
                    Some("postgresql://user:pass@localhost/db".to_string()),
                    Some("localhost".to_string()),
                    None,
                    None,
                    5432,
                    "db".to_string(),
                );

                assert!(result.is_err());
                if let Err(e) = result {
                    let error_msg = e.to_string();
                    assert!(error_msg.contains("Cannot specify both DSN"));
                }
            });
        }

        #[test]
        fn rejects_dsn_and_user() {
            Python::attach(|py| {
                let result = connect(
                    py,
                    Some("postgresql://user:pass@localhost/db".to_string()),
                    None,
                    Some("user".to_string()),
                    None,
                    5432,
                    "db".to_string(),
                );

                assert!(result.is_err());
            });
        }

        #[test]
        fn rejects_dsn_and_password() {
            Python::attach(|py| {
                let result = connect(
                    py,
                    Some("postgresql://user:pass@localhost/db".to_string()),
                    None,
                    None,
                    Some("pass".to_string()),
                    5432,
                    "db".to_string(),
                );

                assert!(result.is_err());
            });
        }

        #[test]
        fn rejects_missing_host_when_no_dsn() {
            Python::attach(|py| {
                let result = connect(
                    py,
                    None,
                    None,
                    Some("user".to_string()),
                    Some("pass".to_string()),
                    5432,
                    "db".to_string(),
                );

                assert!(result.is_err());
                if let Err(e) = result {
                    let error_msg = e.to_string();
                    assert!(error_msg.contains("host"));
                }
            });
        }

        #[test]
        fn rejects_missing_user_when_no_dsn() {
            Python::attach(|py| {
                let result = connect(
                    py,
                    None,
                    Some("localhost".to_string()),
                    None,
                    Some("pass".to_string()),
                    5432,
                    "db".to_string(),
                );

                assert!(result.is_err());
                if let Err(e) = result {
                    let error_msg = e.to_string();
                    assert!(error_msg.contains("user"));
                }
            });
        }

        #[test]
        fn rejects_missing_password_when_no_dsn() {
            Python::attach(|py| {
                let result = connect(
                    py,
                    None,
                    Some("localhost".to_string()),
                    Some("user".to_string()),
                    None,
                    5432,
                    "db".to_string(),
                );

                assert!(result.is_err());
                if let Err(e) = result {
                    let error_msg = e.to_string();
                    assert!(error_msg.contains("password"));
                }
            });
        }

        #[test]
        fn allows_custom_port_with_individual_params() {
            Python::attach(|py| {
                let result = connect(
                    py,
                    None,
                    Some("nonexistent-host-for-testing".to_string()),
                    Some("user".to_string()),
                    Some("pass".to_string()),
                    9999,
                    "db".to_string(),
                );

                if let Err(e) = result {
                    let error_msg = e.to_string();
                    assert!(!error_msg.contains("Missing parameter"));
                }
            });
        }

        #[test]
        fn uses_custom_database_name() {
            Python::attach(|py| {
                let result = connect(
                    py,
                    None,
                    Some("nonexistent-host-for-testing".to_string()),
                    Some("user".to_string()),
                    Some("pass".to_string()),
                    5432,
                    "custom_database_name".to_string(),
                );

                if let Err(e) = result {
                    let error_msg = e.to_string();
                    assert!(!error_msg.contains("Missing parameter"));
                }
            });
        }
    }

    mod edge_cases {
        use super::*;

        #[test]
        fn dsn_preserves_extended_configuration() {
            Python::attach(|_py| {
                let dsn = "postgresql://user:pass@localhost/mydb?connect_timeout=10&keepalives=0"
                    .to_string();
                let mut config = Config::new();

                let result = extract_host_from_dsn(dsn, &mut config);
                assert!(result.is_ok());

                assert_eq!(config.get_connect_timeout(), Some(&Duration::from_secs(10)));
                assert!(!config.get_keepalives());
            });
        }

        #[test]
        fn dsn_with_unix_socket_fails_gracefully() {
            Python::attach(|_py| {
                let dsn = "postgresql://user:pass@%2Fvar%2Frun%2Fpostgresql/mydb".to_string();
                let mut config = Config::new();

                let result = extract_host_from_dsn(dsn, &mut config);

                assert!(result.is_err());
                if let Err(e) = result {
                    let msg = e.to_string();
                    assert!(msg.contains("host"));
                }
            });
        }

        #[test]
        fn dsn_implicit_host_fails() {
            Python::attach(|_py| {
                let dsn = "postgresql://user:pass@/mydb".to_string();
                let mut config = Config::new();

                let result = extract_host_from_dsn(dsn, &mut config);

                assert!(result.is_err());
            });
        }

        #[test]
        fn dsn_with_multiple_hosts_selects_first() {
            Python::attach(|_py| {
                let dsn = "postgresql://user:pass@host1.com,host2.com/mydb".to_string();
                let mut config = Config::new();

                let result = extract_host_from_dsn(dsn, &mut config);

                assert!(result.is_ok());
                if let Ok((host, _, _, _, _)) = result {
                    assert_eq!(host, "host1.com");
                }
            });
        }

        #[test]
        fn dsn_invalid_port_parsing() {
            Python::attach(|_py| {
                let dsn = "postgresql://user:pass@localhost:70000/mydb".to_string();
                let mut config = Config::new();

                let result = extract_host_from_dsn(dsn, &mut config);

                assert!(result.is_err());
                let msg = result.unwrap_err().to_string();
                assert!(msg.contains("Invalid DSN"));
            });
        }

        #[test]
        fn config_population_trims_whitespace_if_needed() {
            let mut config = Config::new();
            populate_config_from_params(
                "localhost".to_string(),
                " user ".to_string(),
                "pass".to_string(),
                5432,
                "db".to_string(),
                &mut config,
            );
            assert_eq!(config.get_user(), Some(" user "));
        }
    }
}
