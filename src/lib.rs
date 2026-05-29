use pyo3::prelude::*;
use regex::Regex;
use std::collections::HashMap;
use std::sync::RwLock;
use once_cell::sync::Lazy;

#[derive(Clone)]
enum CacheEntry {
    Rust(Regex),
    Python(PyObject),
    Error(String),
}

// PyObject is thread-safe and Send/Sync because accessing it requires holding the GIL.
static REGEX_CACHE: Lazy<RwLock<HashMap<String, CacheEntry>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

fn clean_path_str(path: &str) -> String {
    path.replace('\\', "/")
}

fn compile_pattern_internal(pattern: &str) -> Result<Regex, regex::Error> {
    let stripped = pattern.trim();
    // Anchor at the start to mimic Python's re.match behavior
    let anchored = if stripped.starts_with('^') || stripped.starts_with("\\A") {
        stripped.to_string()
    } else {
        format!("^{}", stripped)
    };
    Regex::new(&anchored)
}

fn raise_re_error(py: Python<'_>, msg: String) -> PyErr {
    if let Ok(re_module) = py.import_bound("re") {
        if let Ok(re_error) = re_module.getattr("error") {
            if let Ok(err_val) = re_error.call1((msg.clone(),)) {
                return PyErr::from_value_bound(err_val);
            }
        }
    }
    pyo3::exceptions::PyValueError::new_err(msg)
}

fn try_python_compile(py: Python<'_>, pattern: &str) -> Result<PyObject, PyErr> {
    let stripped = pattern.trim();
    let re_module = py.import_bound("re")?;
    let compile_fn = re_module.getattr("compile")?;
    let py_re = compile_fn.call1((stripped,))?;
    Ok(py_re.to_object(py))
}

#[pyfunction]
#[pyo3(signature = (include_pattern, path))]
fn object_in_path(py: Python<'_>, include_pattern: Option<String>, path: String) -> PyResult<bool> {
    let pattern = match include_pattern {
        None => return Ok(true),
        Some(p) => p,
    };

    // 1. Fast path: Acquire read lock to lookup in cache
    {
        let cache = REGEX_CACHE.read().unwrap();
        if let Some(entry) = cache.get(&pattern) {
            match entry {
                CacheEntry::Rust(re) => {
                    let cleaned = clean_path_str(&path);
                    return Ok(re.is_match(&cleaned));
                }
                CacheEntry::Python(py_re_obj) => {
                    let cleaned = clean_path_str(&path);
                    let py_re = py_re_obj.bind(py);
                    let match_result = py_re.call_method1("match", (cleaned,))?;
                    return Ok(!match_result.is_none());
                }
                CacheEntry::Error(err_msg) => {
                    return Err(raise_re_error(py, err_msg.clone()));
                }
            }
        }
    }

    // 2. Slow path: Compile and insert with write lock
    let cleaned = clean_path_str(&path);
    let compile_result = compile_pattern_internal(&pattern);

    let cache_entry = match compile_result {
        Ok(re) => CacheEntry::Rust(re),
        Err(rust_err) => {
            // Rust compilation failed (e.g. look-around unsupported).
            // Attempt fallback compilation in Python.
            match try_python_compile(py, &pattern) {
                Ok(py_re_obj) => CacheEntry::Python(py_re_obj),
                Err(_) => {
                    // Fails in both Rust and Python; compile error is real.
                    CacheEntry::Error(format!(
                        "Invalid regex pattern '{}': regex parse error:\n{}",
                        pattern, rust_err
                    ))
                }
            }
        }
    };

    {
        let mut cache = REGEX_CACHE.write().unwrap();
        cache.insert(pattern.clone(), cache_entry.clone());
    }

    match cache_entry {
        CacheEntry::Rust(re) => Ok(re.is_match(&cleaned)),
        CacheEntry::Python(py_re_obj) => {
            let py_re = py_re_obj.bind(py);
            let match_result = py_re.call_method1("match", (cleaned,))?;
            Ok(!match_result.is_none())
        }
        CacheEntry::Error(err_msg) => Err(raise_re_error(py, err_msg)),
    }
}

#[pymodule]
fn _dbt_bouncer(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(object_in_path, m)?)?;
    Ok(())
}
