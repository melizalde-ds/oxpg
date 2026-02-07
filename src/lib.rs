mod client;

use pyo3::prelude::*;

#[pymodule]
fn oxpg(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(client::connect, m)?)?;

    m.add_class::<client::Client>()?;

    m.add_function(wrap_pyfunction!(sum_as_string, m)?)?;

    Ok(())
}

#[pyfunction]
fn sum_as_string(a: usize, b: usize) -> PyResult<String> {
    Ok((a + b).to_string())
}
