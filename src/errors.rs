use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3_stub_gen::create_exception;
use thiserror::Error;

create_exception!(oxpg, Error, PyException);
create_exception!(oxpg, InterfaceError, Error);
create_exception!(oxpg, DatabaseError, Error);
create_exception!(oxpg, DataError, DatabaseError);
create_exception!(oxpg, OperationalError, DatabaseError);
create_exception!(oxpg, InternalError, DatabaseError);

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
    fn from(err: OxpgError) -> PyErr {
        let msg = err.to_string();
        match err {
            OxpgError::MissingParameter(_)
            | OxpgError::InvalidParameter(_)
            | OxpgError::InvalidDsn(_) => PyErr::new::<InterfaceError, _>(msg),

            OxpgError::ConnectionFailed(_) | OxpgError::RuntimeFailed(_) => {
                PyErr::new::<OperationalError, _>(msg)
            }

            OxpgError::QueryFailed(_) | OxpgError::ExecutionError(_) => {
                PyErr::new::<DatabaseError, _>(msg)
            }

            OxpgError::UnsupportedType(_) | OxpgError::DataConversionError(_) => {
                PyErr::new::<DataError, _>(msg)
            }

            OxpgError::Unexpected(_) => PyErr::new::<InternalError, _>(msg),
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
