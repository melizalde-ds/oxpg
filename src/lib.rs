mod client;
mod errors;

use pyo3::prelude::*;
use pyo3_stub_gen::define_stub_info_gatherer;

#[pymodule]
fn oxpg(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(client::connect, m)?)?;
    m.add_class::<client::Client>()?;
    Ok(())
}

define_stub_info_gatherer!(stub_info);
