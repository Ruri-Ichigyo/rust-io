use std::path::PathBuf;
use std::sync::LazyLock;
use std::time::Duration;

use bytes::Bytes;
use io_core::{IoEngine, IoError, IoResult, Uri};
use pyo3::exceptions::{
    PyConnectionError, PyFileNotFoundError, PyOSError, PyPermissionError, PyRuntimeError,
    PyTimeoutError, PyTypeError, PyValueError,
};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyString, PyTuple};

static ENGINE: LazyLock<Result<IoEngine, String>> =
    LazyLock::new(|| IoEngine::new().map_err(|error| error.to_string()));

fn engine() -> PyResult<&'static IoEngine> {
    ENGINE
        .as_ref()
        .map_err(|error| PyRuntimeError::new_err(error.clone()))
}

fn timeout(timeout_ms: Option<u64>) -> Option<Duration> {
    timeout_ms.map(Duration::from_millis)
}

fn to_py_error(error: IoError) -> PyErr {
    match error {
        IoError::InvalidUri(message) | IoError::InvalidConfig(message) => {
            PyValueError::new_err(message)
        }
        IoError::NotFound(message) => PyFileNotFoundError::new_err(message),
        IoError::PermissionDenied(message) => PyPermissionError::new_err(message),
        IoError::Timeout(duration) => {
            PyTimeoutError::new_err(format!("operation timed out after {duration:?}"))
        }
        IoError::Http { message, .. } | IoError::S3 { message, .. } => {
            PyConnectionError::new_err(message)
        }
        IoError::Io(error) => PyOSError::new_err(error.to_string()),
        IoError::Unsupported(message) | IoError::Other(message) => PyRuntimeError::new_err(message),
        IoError::AllFailed(causes) => {
            let details: Vec<String> = causes
                .into_iter()
                .map(|(uri, error)| format!("{uri}: {error}"))
                .collect();
            PyRuntimeError::new_err(format!("all candidates failed: {}", details.join("; ")))
        }
    }
}

fn extract_bytes(data: &Bound<'_, PyAny>) -> PyResult<Bytes> {
    if let Ok(bytes) = data.cast::<PyBytes>() {
        return Ok(Bytes::copy_from_slice(bytes.as_bytes()));
    }
    if let Ok(text) = data.cast::<PyString>() {
        return Ok(Bytes::copy_from_slice(text.to_str()?.as_bytes()));
    }
    let bytes = data
        .extract::<Vec<u8>>()
        .map_err(|_| PyTypeError::new_err("data must be bytes, bytearray, memoryview, or str"))?;
    Ok(Bytes::from(bytes))
}

fn parse_uris(uris: Vec<String>) -> PyResult<Vec<Uri>> {
    uris.into_iter()
        .map(|uri| Uri::parse(&uri))
        .collect::<IoResult<Vec<_>>>()
        .map_err(to_py_error)
}

#[pyfunction]
#[pyo3(signature = (uri, timeout_ms=None))]
pub fn read<'py>(
    py: Python<'py>,
    uri: String,
    timeout_ms: Option<u64>,
) -> PyResult<Bound<'py, PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let uri = Uri::parse(&uri).map_err(to_py_error)?;
        let bytes = engine()?
            .read(&uri, timeout(timeout_ms))
            .await
            .map_err(to_py_error)?;
        Python::attach(|py| Ok(PyBytes::new(py, &bytes).into_any().unbind()))
    })
}

#[pyfunction]
#[pyo3(signature = (uri, offset, length, timeout_ms=None))]
pub fn read_range<'py>(
    py: Python<'py>,
    uri: String,
    offset: u64,
    length: u64,
    timeout_ms: Option<u64>,
) -> PyResult<Bound<'py, PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let uri = Uri::parse(&uri).map_err(to_py_error)?;
        let bytes = engine()?
            .read_range(&uri, offset, length, timeout(timeout_ms))
            .await
            .map_err(to_py_error)?;
        Python::attach(|py| Ok(PyBytes::new(py, &bytes).into_any().unbind()))
    })
}

#[pyfunction]
#[pyo3(signature = (uri, data, timeout_ms=None))]
pub fn write<'py>(
    py: Python<'py>,
    uri: String,
    data: &Bound<'py, PyAny>,
    timeout_ms: Option<u64>,
) -> PyResult<Bound<'py, PyAny>> {
    let data = extract_bytes(data)?;
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let uri = Uri::parse(&uri).map_err(to_py_error)?;
        engine()?
            .write(&uri, data, timeout(timeout_ms))
            .await
            .map_err(to_py_error)?;
        Ok(())
    })
}

#[pyfunction]
#[pyo3(signature = (uri, timeout_ms=None))]
pub fn stat<'py>(
    py: Python<'py>,
    uri: String,
    timeout_ms: Option<u64>,
) -> PyResult<Bound<'py, PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let uri = Uri::parse(&uri).map_err(to_py_error)?;
        let metadata = engine()?
            .stat(&uri, timeout(timeout_ms))
            .await
            .map_err(to_py_error)?;
        Python::attach(|py| {
            let dict = PyDict::new(py);
            dict.set_item("uri", metadata.uri.to_string())?;
            dict.set_item("len", metadata.len)?;
            dict.set_item("is_file", metadata.is_file)?;
            dict.set_item("is_dir", metadata.is_dir)?;
            dict.set_item("modified", metadata.modified)?;
            dict.set_item("content_type", metadata.content_type)?;
            dict.set_item("etag", metadata.etag)?;
            Ok(dict.into_any().unbind())
        })
    })
}

#[pyfunction]
#[pyo3(signature = (uri, timeout_ms=None))]
pub fn list<'py>(
    py: Python<'py>,
    uri: String,
    timeout_ms: Option<u64>,
) -> PyResult<Bound<'py, PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let uri = Uri::parse(&uri).map_err(to_py_error)?;
        let entries = engine()?
            .list(&uri, timeout(timeout_ms))
            .await
            .map_err(to_py_error)?;
        Python::attach(|py| {
            let list = PyList::empty(py);
            for entry in entries {
                list.append(entry)?;
            }
            Ok(list.into_any().unbind())
        })
    })
}

#[pyfunction]
#[pyo3(signature = (uri, timeout_ms=None))]
pub fn delete<'py>(
    py: Python<'py>,
    uri: String,
    timeout_ms: Option<u64>,
) -> PyResult<Bound<'py, PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let uri = Uri::parse(&uri).map_err(to_py_error)?;
        engine()?
            .delete(&uri, timeout(timeout_ms))
            .await
            .map_err(to_py_error)?;
        Ok(())
    })
}

#[pyfunction]
#[pyo3(signature = (source, destination, timeout_ms=None))]
pub fn copy<'py>(
    py: Python<'py>,
    source: String,
    destination: String,
    timeout_ms: Option<u64>,
) -> PyResult<Bound<'py, PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let source = Uri::parse(&source).map_err(to_py_error)?;
        let destination = Uri::parse(&destination).map_err(to_py_error)?;
        engine()?
            .copy(&source, &destination, timeout(timeout_ms))
            .await
            .map_err(to_py_error)?;
        Ok(())
    })
}

fn item_dict<'py>(
    py: Python<'py>,
    uri: &str,
    error: Option<IoError>,
) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("ok", error.is_none())?;
    dict.set_item("uri", uri)?;
    dict.set_item("error", error.map(|error| error.to_string()))?;
    Ok(dict)
}

#[pyfunction]
#[pyo3(signature = (uris, limit=8, timeout_ms=None))]
pub fn read_many<'py>(
    py: Python<'py>,
    uris: Vec<String>,
    limit: usize,
    timeout_ms: Option<u64>,
) -> PyResult<Bound<'py, PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let parsed = parse_uris(uris)?;
        let results = engine()?
            .read_many(&parsed, limit, timeout(timeout_ms))
            .await
            .map_err(to_py_error)?;
        Python::attach(|py| {
            let list = PyList::empty(py);
            for (uri, result) in results {
                let dict = PyDict::new(py);
                match result {
                    Ok(bytes) => {
                        dict.set_item("ok", true)?;
                        dict.set_item("uri", uri.to_string())?;
                        dict.set_item("bytes", PyBytes::new(py, &bytes))?;
                        dict.set_item("error", py.None())?;
                    }
                    Err(error) => {
                        dict.set_item("ok", false)?;
                        dict.set_item("uri", uri.to_string())?;
                        dict.set_item("bytes", py.None())?;
                        dict.set_item("error", error.to_string())?;
                    }
                }
                list.append(dict)?;
            }
            Ok(list.into_any().unbind())
        })
    })
}

#[pyfunction]
#[pyo3(signature = (items, limit=8, timeout_ms=None))]
pub fn write_many<'py>(
    py: Python<'py>,
    items: Vec<Bound<'py, PyAny>>,
    limit: usize,
    timeout_ms: Option<u64>,
) -> PyResult<Bound<'py, PyAny>> {
    let mut parsed = Vec::with_capacity(items.len());
    for item in items {
        let tuple = item
            .cast::<PyTuple>()
            .map_err(|_| PyTypeError::new_err("items must contain (uri, data) tuples"))?;
        let uri: String = tuple.get_item(0)?.extract()?;
        let data = extract_bytes(&tuple.get_item(1)?)?;
        parsed.push((uri, data));
    }

    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let mut pairs = Vec::with_capacity(parsed.len());
        for (uri, data) in parsed {
            pairs.push((Uri::parse(&uri).map_err(to_py_error)?, data));
        }
        let results = engine()?
            .write_many(&pairs, limit, timeout(timeout_ms))
            .await
            .map_err(to_py_error)?;
        Python::attach(|py| {
            let list = PyList::empty(py);
            for (uri, result) in results {
                list.append(item_dict(py, &uri.to_string(), result.err())?)?;
            }
            Ok(list.into_any().unbind())
        })
    })
}

#[pyfunction]
#[pyo3(signature = (pairs, limit=8, timeout_ms=None))]
pub fn copy_many<'py>(
    py: Python<'py>,
    pairs: Vec<(String, String)>,
    limit: usize,
    timeout_ms: Option<u64>,
) -> PyResult<Bound<'py, PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let mut parsed = Vec::with_capacity(pairs.len());
        for (from, to) in pairs {
            parsed.push((
                Uri::parse(&from).map_err(to_py_error)?,
                Uri::parse(&to).map_err(to_py_error)?,
            ));
        }
        let results = engine()?
            .copy_many(&parsed, limit, timeout(timeout_ms))
            .await
            .map_err(to_py_error)?;
        Python::attach(|py| {
            let list = PyList::empty(py);
            for (uri, result) in results {
                list.append(item_dict(py, &uri.to_string(), result.err())?)?;
            }
            Ok(list.into_any().unbind())
        })
    })
}

#[pyfunction]
#[pyo3(signature = (uris, timeout_ms=None))]
pub fn read_any<'py>(
    py: Python<'py>,
    uris: Vec<String>,
    timeout_ms: Option<u64>,
) -> PyResult<Bound<'py, PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let parsed = parse_uris(uris)?;
        let (uri, bytes) = engine()?
            .read_any(&parsed, timeout(timeout_ms))
            .await
            .map_err(to_py_error)?;
        Python::attach(|py| {
            let dict = PyDict::new(py);
            dict.set_item("uri", uri.to_string())?;
            dict.set_item("bytes", PyBytes::new(py, &bytes))?;
            Ok(dict.into_any().unbind())
        })
    })
}

#[pyfunction]
#[pyo3(signature = (uri, path, timeout_ms=None))]
pub fn download<'py>(
    py: Python<'py>,
    uri: String,
    path: String,
    timeout_ms: Option<u64>,
) -> PyResult<Bound<'py, PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let uri = Uri::parse(&uri).map_err(to_py_error)?;
        engine()?
            .download(&uri, &PathBuf::from(path), timeout(timeout_ms))
            .await
            .map_err(to_py_error)
    })
}

#[pyfunction]
#[pyo3(signature = (path, uri, timeout_ms=None))]
pub fn upload<'py>(
    py: Python<'py>,
    path: String,
    uri: String,
    timeout_ms: Option<u64>,
) -> PyResult<Bound<'py, PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let uri = Uri::parse(&uri).map_err(to_py_error)?;
        engine()?
            .upload(&PathBuf::from(path), &uri, timeout(timeout_ms))
            .await
            .map_err(to_py_error)?;
        Ok(())
    })
}

#[pyfunction]
#[pyo3(signature = (uri, timeout_ms=None))]
pub fn hash<'py>(
    py: Python<'py>,
    uri: String,
    timeout_ms: Option<u64>,
) -> PyResult<Bound<'py, PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let uri = Uri::parse(&uri).map_err(to_py_error)?;
        engine()?
            .hash(&uri, timeout(timeout_ms))
            .await
            .map_err(to_py_error)
    })
}

#[pyfunction]
#[pyo3(signature = (uris, limit=8, timeout_ms=None))]
pub fn hash_many<'py>(
    py: Python<'py>,
    uris: Vec<String>,
    limit: usize,
    timeout_ms: Option<u64>,
) -> PyResult<Bound<'py, PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let parsed = parse_uris(uris)?;
        let results = engine()?
            .hash_many(&parsed, limit, timeout(timeout_ms))
            .await
            .map_err(to_py_error)?;
        Python::attach(|py| {
            let list = PyList::empty(py);
            for (uri, result) in results {
                let dict = PyDict::new(py);
                match result {
                    Ok(value) => {
                        dict.set_item("ok", true)?;
                        dict.set_item("uri", uri.to_string())?;
                        dict.set_item("hash", value)?;
                        dict.set_item("error", py.None())?;
                    }
                    Err(error) => {
                        dict.set_item("ok", false)?;
                        dict.set_item("uri", uri.to_string())?;
                        dict.set_item("hash", py.None())?;
                        dict.set_item("error", error.to_string())?;
                    }
                }
                list.append(dict)?;
            }
            Ok(list.into_any().unbind())
        })
    })
}

/// General async I/O surface: local, HTTP(S), and S3 behind one URI model.
#[pymodule]
#[pyo3(name = "io", submodule)]
pub mod io {
    #[pymodule_export]
    use super::{
        copy, copy_many, delete, download, hash, hash_many, list, read, read_any, read_many,
        read_range, stat, upload, write, write_many,
    };
}
