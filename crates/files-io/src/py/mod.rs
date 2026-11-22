use pyo3::prelude::*;

pub mod dedup;
pub mod file;
pub mod parallel;
pub mod utils;

/// Python entry point for the `rust_io.files` submodule.
///
/// `submodule` skips the `PyInit_files` symbol: this crate is only ever linked
/// into `rust_io`, which exposes the module as `rust_io.files`.
#[pymodule]
#[pyo3(name = "files", submodule)]
pub mod files {
    #[pymodule_export]
    use super::dedup::{batch_hash, check_duplicate};
    #[pymodule_export]
    use super::file::File;
    #[pymodule_export]
    use super::parallel::{batch_compress, batch_copy, batch_copy_async};
    #[pymodule_export]
    use super::utils::{compute_sha256, validate_pdf_magic, write_file};
}
