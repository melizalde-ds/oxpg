use pyo3::{PyErr, PyResult, pyclass, pyfunction, pymethods};

#[pyclass]
struct Client {
    host: String,
    port: u16,
    db: String,
    user: String,
    dsn: String,
}

#[pyfunction]
#[pyo3(signature = (dsn=None, host=None, user=None, password=None, port=5432, db="postgres".to_string()))]
fn connect(
    dsn: Option<String>,
    host: Option<String>,
    user: Option<String>,
    password: Option<String>,
    port: u16,
    db: String,
) -> PyResult<Client> {
    let connection_string = match dsn {
        Some(s) => s,
        None => build_dsn(host, user, password, port, db)?,
    };
    todo!()
}

fn build_dsn(
    host: Option<String>,
    user: Option<String>,
    password: Option<String>,
    port: u16,
    db: String,
) -> PyResult<String> {
    let host =
        host.ok_or_else(|| PyErr::new::<pyo3::exceptions::PyValueError, _>("host is required"))?;
    let user =
        user.ok_or_else(|| PyErr::new::<pyo3::exceptions::PyValueError, _>("user is required"))?;
    let password = password
        .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyValueError, _>("password is required"))?;

    Ok(format!(
        "postgresql://{}:{}@{}:{}/{}",
        user, password, host, port, db
    ))
}

#[pymethods]
impl Client {
    #[new]
    #[pyo3(signature = (host, user, password, port=5432, db="postgres".to_string()))]
    fn new(host: String, user: String, password: String, port: u16, db: String) -> Self {
        Client {
            host: host.clone(),
            port,
            db: db.clone(),
            user: user.clone(),
            dsn: format!(
                "host={} port={} dbname={} user={} password={}",
                host, port, db, user, password
            ),
        }
    }
}
