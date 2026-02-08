use pyo3_stub_gen::Result;

fn main() -> Result<()> {
    let stub = oxpg::stub_info()?;
    stub.generate()?;
    Ok(())
}
