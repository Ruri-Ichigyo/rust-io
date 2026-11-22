use crate::backends::{FileOps, local::LocalBackend, s3::S3Backend};
use crate::error::FileError;
use crate::hash;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyString};

#[derive(Clone)]
enum Backend {
    Local(LocalBackend),
    S3(S3Backend),
}

#[pyclass(module = "rust_io.files")]
pub struct File {
    path: String,
    backend: Backend,
}

impl File {
    fn ops(&self) -> &dyn FileOps {
        match &self.backend {
            Backend::Local(b) => b,
            Backend::S3(b) => b,
        }
    }
}

#[pymethods]
impl File {
    #[new]
    #[pyo3(signature = (path, access_key=None, secret_key=None, endpoint=None, region=None))]
    fn new(
        path: &str,
        access_key: Option<&str>,
        secret_key: Option<&str>,
        endpoint: Option<&str>,
        region: Option<&str>,
    ) -> PyResult<Self> {
        if path.starts_with("s3://") {
            let ak = access_key.ok_or_else(|| {
                PyErr::new::<pyo3::exceptions::PyValueError, _>("access_key required for S3 paths")
            })?;
            let sk = secret_key.ok_or_else(|| {
                PyErr::new::<pyo3::exceptions::PyValueError, _>("secret_key required for S3 paths")
            })?;
            let backend = S3Backend::new(ak, sk, endpoint, region)?;
            Ok(Self {
                path: path.to_string(),
                backend: Backend::S3(backend),
            })
        } else {
            Ok(Self {
                path: path.to_string(),
                backend: Backend::Local(LocalBackend::new()),
            })
        }
    }

    #[getter]
    fn path(&self) -> &str {
        &self.path
    }

    fn __repr__(&self) -> String {
        format!("File({:?})", self.path)
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __exit__(
        &mut self,
        _exc_type: Option<&Bound<'_, PyAny>>,
        _exc_value: Option<&Bound<'_, PyAny>>,
        _traceback: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        Ok(false)
    }

    #[pyo3(signature = (as_text=false))]
    fn read(&self, as_text: bool) -> PyResult<Py<PyAny>> {
        let data = self.ops().read_all(&self.path)?;
        Python::attach(|py| {
            if as_text {
                let s = String::from_utf8(data)
                    .map_err(|e| FileError::Other(format!("UTF-8 decode error: {e}")))?;
                Ok(s.into_pyobject(py)?.unbind().into_any())
            } else {
                Ok(PyBytes::new(py, &data).into_any().unbind())
            }
        })
    }

    fn read_chunk(&self, offset: u64, size: u64) -> PyResult<Py<PyAny>> {
        let data = self.ops().read_chunk(&self.path, offset, size)?;
        Python::attach(|py| Ok(PyBytes::new(py, &data).into_any().unbind()))
    }

    fn write(&self, data: &Bound<'_, PyAny>) -> PyResult<()> {
        // Borrow the Python buffer directly instead of materializing a Vec.
        if let Ok(bytes) = data.cast::<PyBytes>() {
            self.ops().write(&self.path, bytes.as_bytes(), true)?;
            return Ok(());
        }
        if let Ok(text) = data.cast::<PyString>() {
            self.ops()
                .write(&self.path, text.to_str()?.as_bytes(), true)?;
            return Ok(());
        }
        // Keep accepting bytearray/memoryview (and other buffers) as before.
        let bytes = data.extract::<Vec<u8>>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>("write() accepts bytes or str")
        })?;
        self.ops().write(&self.path, &bytes, true)?;
        Ok(())
    }

    fn exists(&self) -> PyResult<bool> {
        Ok(self.ops().exists(&self.path)?)
    }

    fn metadata<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let meta = self.ops().metadata(&self.path)?;
        let dict = PyDict::new(py);
        dict.set_item("size", meta.size)?;
        dict.set_item("mtime", meta.mtime)?;
        dict.set_item("is_file", meta.is_file)?;
        dict.set_item("is_dir", meta.is_dir)?;
        dict.set_item("is_symlink", meta.is_symlink)?;
        dict.set_item("permissions", &meta.permissions)?;
        for (k, v) in &meta.extra {
            dict.set_item(k, v)?;
        }
        Ok(dict)
    }

    fn rename(&self, dst: &str) -> PyResult<()> {
        self.ops().rename(&self.path, dst)?;
        Ok(())
    }

    fn copy(&self, dst: &str) -> PyResult<()> {
        self.ops().copy(&self.path, dst)?;
        Ok(())
    }

    fn remove(&self) -> PyResult<()> {
        self.ops().remove(&self.path)?;
        Ok(())
    }

    fn remove_dir_all(&self) -> PyResult<()> {
        self.ops().remove_dir_all(&self.path)?;
        Ok(())
    }

    fn list_dir(&self) -> PyResult<Vec<String>> {
        Ok(self.ops().list_dir(&self.path)?)
    }

    fn content_hash(&self, py: Python<'_>) -> PyResult<String> {
        match &self.backend {
            Backend::Local(_) => {
                let path = self.path.clone();
                py.detach(move || hash::hash_file(std::path::Path::new(&path)))
                    .map_err(PyErr::from)
            }
            Backend::S3(_) => {
                let data = self.ops().read_all(&self.path)?;
                Ok(hash::hash_bytes(&data))
            }
        }
    }

    fn compress(&self, output_path: &str, format: &str) -> PyResult<u64> {
        let count = match format {
            "zip" => crate::archive::zip::compress_dir(&self.path, output_path)?,
            "tar" => crate::archive::tar_gz::compress_tar(&self.path, output_path)?,
            "tar.gz" | "tgz" => crate::archive::tar_gz::compress_tar_gz(&self.path, output_path)?,
            _ => {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                    "unsupported format: {format}. Use zip, tar, or tar.gz"
                )));
            }
        };
        Ok(count)
    }

    fn extract(&self, output_dir: &str) -> PyResult<u64> {
        let path_lower = self.path.to_lowercase();
        let count = if path_lower.ends_with(".zip") {
            crate::archive::zip::extract(&self.path, output_dir)?
        } else if path_lower.ends_with(".tar.gz") || path_lower.ends_with(".tgz") {
            crate::archive::tar_gz::extract_tar_gz(&self.path, output_dir)?
        } else if path_lower.ends_with(".tar") {
            crate::archive::tar_gz::extract_tar(&self.path, output_dir)?
        } else {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "cannot detect archive format from path: {}",
                self.path
            )));
        };
        Ok(count)
    }

    fn copy_async<'py>(&self, py: Python<'py>, dst: String) -> PyResult<Bound<'py, PyAny>> {
        let path = self.path.clone();
        let backend = self.backend.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            tokio::task::spawn_blocking(move || {
                let ops: &dyn FileOps = match &backend {
                    Backend::Local(b) => b,
                    Backend::S3(b) => b,
                };
                ops.copy(&path, &dst)
            })
            .await
            .map_err(FileError::TaskJoin)??;
            Ok(())
        })
    }

    fn compress_async<'py>(
        &self,
        py: Python<'py>,
        output_path: String,
        format: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let dir = self.path.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let count = tokio::task::spawn_blocking(move || match format.as_str() {
                "zip" => crate::archive::zip::compress_dir(&dir, &output_path),
                "tar" => crate::archive::tar_gz::compress_tar(&dir, &output_path),
                "tar.gz" | "tgz" => crate::archive::tar_gz::compress_tar_gz(&dir, &output_path),
                _ => Err(FileError::Archive(format!("unsupported format: {format}"))),
            })
            .await
            .map_err(FileError::TaskJoin)??;
            Ok(count)
        })
    }

    fn extract_async<'py>(
        &self,
        py: Python<'py>,
        output_dir: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let path = self.path.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let count = tokio::task::spawn_blocking(move || {
                let path_lower = path.to_lowercase();
                if path_lower.ends_with(".zip") {
                    crate::archive::zip::extract(&path, &output_dir)
                } else if path_lower.ends_with(".tar.gz") || path_lower.ends_with(".tgz") {
                    crate::archive::tar_gz::extract_tar_gz(&path, &output_dir)
                } else if path_lower.ends_with(".tar") {
                    crate::archive::tar_gz::extract_tar(&path, &output_dir)
                } else {
                    Err(FileError::Archive(format!("cannot detect format: {path}")))
                }
            })
            .await
            .map_err(FileError::TaskJoin)??;
            Ok(count)
        })
    }
}
