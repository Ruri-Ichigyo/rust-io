use crate::hash;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use rayon::prelude::*;
use std::collections::HashMap;
use std::path::Path;

#[pyfunction]
pub fn check_duplicate(
    py: Python<'_>,
    file_path: &str,
    known_hashes: Vec<String>,
) -> PyResult<Py<PyAny>> {
    let path = file_path.to_owned();
    let file_hash = py
        .detach(move || hash::hash_file(Path::new(&path)))
        .map_err(PyErr::from)?;
    let is_dup = known_hashes.contains(&file_hash);
    Python::attach(|py| {
        let dict = PyDict::new(py);
        dict.set_item("hash", &file_hash)?;
        dict.set_item("is_duplicate", is_dup)?;
        Ok(dict.into_any().unbind())
    })
}

/// Hash every path in parallel, preserving one outcome per input path.
fn hash_paths(file_paths: &[String]) -> Vec<(String, Result<String, String>)> {
    file_paths
        .par_iter()
        .map(|path| {
            let result = hash::hash_file(Path::new(path)).map_err(|e| e.to_string());
            (path.clone(), result)
        })
        .collect()
}

#[pyfunction]
pub fn batch_hash(py: Python<'_>, file_paths: Vec<String>) -> PyResult<Py<PyAny>> {
    let results = py.detach(|| hash_paths(&file_paths));
    let mut hashes = HashMap::with_capacity(results.len());
    let mut errors = HashMap::new();
    for (path, result) in results {
        match result {
            Ok(file_hash) => {
                hashes.insert(path, file_hash);
            }
            Err(error) => {
                errors.insert(path, error);
            }
        }
    }
    Python::attach(|py| {
        let dict = PyDict::new(py);
        dict.set_item("hashes", &hashes)?;
        dict.set_item("errors", &errors)?;
        Ok(dict.into_any().unbind())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn hash_paths_hashes_files_and_reports_missing_paths() {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("files_io_hash_paths_{suffix}"));
        fs::create_dir_all(&dir).unwrap();
        let present = dir.join("present.txt");
        fs::write(&present, b"abc").unwrap();
        let missing = dir.join("missing.txt");

        let paths = vec![
            present.to_string_lossy().into_owned(),
            missing.to_string_lossy().into_owned(),
        ];
        let results: HashMap<_, _> = hash_paths(&paths).into_iter().collect();

        assert_eq!(
            results[&paths[0]].as_deref().unwrap(),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert!(results[&paths[1]].is_err());

        fs::remove_dir_all(&dir).unwrap();
    }
}
