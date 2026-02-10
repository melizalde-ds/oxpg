use crate::errors::OxpgError;
use chrono::{DateTime, NaiveDate, Utc};
use pyo3::prelude::*;
use pyo3::types::{
    PyBool, PyByteArray, PyBytes, PyDate, PyDateTime, PyDelta, PyDict, PyFloat, PyInt, PyNone,
    PyString, PyTime, PyTuple,
};
use tokio_postgres::types::{ToSql, Type};
use tokio_postgres::{Row, Statement};

pub(crate) fn prepare_params<'a>(
    statement: &Statement,
    args: &Bound<'a, PyTuple>,
) -> PyResult<Vec<Box<dyn ToSql + Sync>>> {
    let mut params: Vec<Box<dyn ToSql + Sync>> = Vec::new();

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
    Ok(params)
}

pub(crate) fn row_to_dict<'a>(py: Python<'a>, row: &Row) -> PyResult<Bound<'a, PyDict>> {
    let row_dict = PyDict::new(py);

    for (idx, column) in row.columns().iter().enumerate() {
        let value = match *column.type_() {
            Type::BOOL => row
                .get::<_, Option<bool>>(idx)
                .into_pyobject(py)
                .map_err(|e| {
                    PyErr::from(OxpgError::DataConversionError(format!(
                        "Failed to convert BOOL column '{}': {:?}",
                        column.name(),
                        e
                    )))
                })?,
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
            Type::INT2 => row
                .get::<_, Option<i16>>(idx)
                .into_pyobject(py)
                .map_err(|e| {
                    PyErr::from(OxpgError::DataConversionError(format!(
                        "Failed to convert INT2 column '{}': {:?}",
                        column.name(),
                        e
                    )))
                })?,
            Type::INT4 => row
                .get::<_, Option<i32>>(idx)
                .into_pyobject(py)
                .map_err(|e| {
                    PyErr::from(OxpgError::DataConversionError(format!(
                        "Failed to convert INT4 column '{}': {:?}",
                        column.name(),
                        e
                    )))
                })?,
            Type::INT8 => row
                .get::<_, Option<i64>>(idx)
                .into_pyobject(py)
                .map_err(|e| {
                    PyErr::from(OxpgError::DataConversionError(format!(
                        "Failed to convert INT8 column '{}': {:?}",
                        column.name(),
                        e
                    )))
                })?,
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
            Type::FLOAT4 => row
                .get::<_, Option<f32>>(idx)
                .into_pyobject(py)
                .map_err(|e| {
                    PyErr::from(OxpgError::DataConversionError(format!(
                        "Failed to convert FLOAT4 column '{}': {:?}",
                        column.name(),
                        e
                    )))
                })?,
            Type::FLOAT8 => row
                .get::<_, Option<f64>>(idx)
                .into_pyobject(py)
                .map_err(|e| {
                    PyErr::from(OxpgError::DataConversionError(format!(
                        "Failed to convert FLOAT8 column '{}': {:?}",
                        column.name(),
                        e
                    )))
                })?,
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

    Ok(row_dict)
}
