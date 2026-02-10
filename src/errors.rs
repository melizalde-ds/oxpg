use pyo3::{
    PyErr,
    exceptions::{PyConnectionError, PyRuntimeError, PyTypeError, PyValueError},
};
use std::convert::From;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum OxpgError {
    #[error("Missing required parameter: {0}")]
    MissingParameter(String),
    #[error("Invalid parameter value: {0}")]
    InvalidParameter(String),
    #[error("Invalid DSN: {0}")]
    InvalidDsn(String),
    #[error("Connection failed: {0}")]
    ConnectionFailed(String),
    #[error("Runtime failed: {0}")]
    RuntimeFailed(String),

    #[error("Query failed: {0}")]
    QueryFailed(String),
    #[error("Database execution failed: {0}")]
    ExecutionError(String),
    #[error("Unsupported Python type: {0}")]
    UnsupportedType(String),
    #[error("Data conversion failed: {0}")]
    DataConversionError(String),

    #[error("Unexpected error: {0}")]
    Unexpected(String),
}

impl From<OxpgError> for PyErr {
    fn from(value: OxpgError) -> Self {
        match value {
            OxpgError::MissingParameter(param) => {
                PyErr::new::<PyValueError, _>(format!("Missing required parameter: {}", param))
            }
            OxpgError::InvalidParameter(param) => {
                PyErr::new::<PyValueError, _>(format!("Invalid parameter value: {}", param))
            }
            OxpgError::InvalidDsn(reason) => {
                PyErr::new::<PyValueError, _>(format!("Invalid DSN: {}", reason))
            }
            OxpgError::ConnectionFailed(reason) => {
                PyErr::new::<PyConnectionError, _>(format!("Connection failed: {}", reason))
            }
            OxpgError::RuntimeFailed(reason) => {
                PyErr::new::<PyRuntimeError, _>(format!("Runtime failed: {}", reason))
            }
            OxpgError::Unexpected(msg) => {
                PyErr::new::<PyRuntimeError, _>(format!("Unexpected error: {}", msg))
            }
            OxpgError::QueryFailed(reason) => {
                PyErr::new::<PyRuntimeError, _>(format!("Query failed: {}", reason))
            }
            OxpgError::ExecutionError(msg) => PyErr::new::<PyRuntimeError, _>(msg),

            OxpgError::UnsupportedType(msg) => PyErr::new::<PyTypeError, _>(msg),
            OxpgError::DataConversionError(msg) => PyErr::new::<PyValueError, _>(msg),
        }
    }
}
