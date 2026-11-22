use pyo3::prelude::*;
use pyo3::types::PyDict;

/// Attach a PyO3 submodule to `parent` and register it in `sys.modules`.
///
/// PyO3's `add_submodule` derives the attribute name from `__name__`, so the
/// fully-qualified name is set only after the attribute has been added.
fn register_submodule(
    parent: &Bound<'_, PyModule>,
    name: &str,
    full_name: &str,
    submodule: &Bound<'_, PyModule>,
) -> PyResult<()> {
    parent.add(name, submodule)?;
    submodule.setattr("__name__", full_name)?;
    parent
        .py()
        .import("sys")?
        .getattr("modules")?
        .cast::<PyDict>()?
        .set_item(full_name, submodule)?;
    Ok(())
}

#[pymodule]
fn rust_io(m: &Bound<'_, PyModule>) -> PyResult<()> {
    let py = m.py();
    let net = pyo3::wrap_pymodule!(net_io::py::net)(py);
    register_submodule(m, "net", "rust_io.net", net.bind(py))?;
    let files = pyo3::wrap_pymodule!(files_io::py::files)(py);
    register_submodule(m, "files", "rust_io.files", files.bind(py))?;
    let io = pyo3::wrap_pymodule!(io_py::io)(py);
    register_submodule(m, "io", "rust_io.io", io.bind(py))?;
    Ok(())
}
