use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3_stub_gen::create_exception;
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

create_exception!(oxpg, Error, PyException);

create_exception!(oxpg, InterfaceError, Error);

create_exception!(oxpg, DatabaseError, Error);

create_exception!(oxpg, DataError, DatabaseError);

create_exception!(oxpg, OperationalError, DatabaseError);

create_exception!(oxpg, InternalError, DatabaseError);

impl From<OxpgError> for PyErr {
    fn from(err: OxpgError) -> PyErr {
        match err {
            OxpgError::MissingParameter(msg) => InterfaceError::new_err(msg),
            OxpgError::InvalidParameter(msg) => InterfaceError::new_err(msg),
            OxpgError::InvalidDsn(msg) => InterfaceError::new_err(msg),

            OxpgError::ConnectionFailed(msg) => OperationalError::new_err(msg),
            OxpgError::RuntimeFailed(msg) => OperationalError::new_err(msg),

            OxpgError::QueryFailed(msg) => DatabaseError::new_err(msg),
            OxpgError::ExecutionError(msg) => DatabaseError::new_err(msg),

            OxpgError::UnsupportedType(msg) => DataError::new_err(msg),
            OxpgError::DataConversionError(msg) => DataError::new_err(msg),

            OxpgError::Unexpected(msg) => InternalError::new_err(msg),
        }
    }
}

pub fn register_exceptions(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("Error", m.py().get_type::<Error>())?;
    m.add("InterfaceError", m.py().get_type::<InterfaceError>())?;
    m.add("DatabaseError", m.py().get_type::<DatabaseError>())?;
    m.add("DataError", m.py().get_type::<DataError>())?;
    m.add("OperationalError", m.py().get_type::<OperationalError>())?;
    m.add("InternalError", m.py().get_type::<InternalError>())?;

    Ok(())
}
