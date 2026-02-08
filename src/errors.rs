use pyo3::PyErr;
use std::convert::From;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum OxpgError {
    #[error("Missing required parameter: {0}")]
    MissingParameter(String),
    #[error("Invalid parameter value: {0}")]
    InvalidParameter(String),
    #[error("Connection failed: {0}")]
    ConnectionFailed(String),

    #[error("Unexpected error: {0}")]
    Unexpected(String),
}

impl From<OxpgError> for PyErr {
    fn from(value: OxpgError) -> Self {
        match value {
            OxpgError::MissingParameter(param) => PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("Missing required parameter: {}", param),
            ),
            OxpgError::InvalidParameter(param) => PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("Invalid parameter value: {}", param),
            ),
            OxpgError::ConnectionFailed(reason) => {
                PyErr::new::<pyo3::exceptions::PyConnectionError, _>(format!(
                    "Connection failed: {}",
                    reason
                ))
            }
            OxpgError::Unexpected(msg) => PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                format!("Unexpected error: {}", msg),
            ),
        }
    }
}
