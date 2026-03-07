use pyo3::exceptions::{PyAttributeError, PyIndexError, PyKeyError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyList, PyTuple};
use serde_json::Value;

/// A generic Python-accessible wrapper around a JSON value.
///
/// - JSON objects support attribute access (`.field`), dict ops (`.items()`, `["key"]`), and iteration.
/// - JSON arrays support indexing (`[0]`), iteration, `len()`, and containment (`in`).
/// - Primitives are returned as native Python types (str, int, float, bool, None).
/// - Accessing `.value` on a primitive returns itself (for Pydantic enum compatibility).
#[pyclass(name = "JsonObj", module = "dbt_artifacts_rs")]
#[derive(Clone, Debug)]
struct JsonObj {
    data: Value,
}

/// Convert a serde_json::Value to a Python object.
/// Primitives become native Python types; objects/arrays become JsonObj.
fn value_to_py(py: Python<'_>, v: &Value) -> PyResult<PyObject> {
    match v {
        Value::Null => Ok(py.None()),
        Value::Bool(b) => Ok(b.into_pyobject(py).unwrap().to_owned().into_any().unbind()),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(i.into_pyobject(py).unwrap().into_any().unbind())
            } else if let Some(f) = n.as_f64() {
                Ok(f.into_pyobject(py).unwrap().into_any().unbind())
            } else {
                Ok(py.None())
            }
        }
        Value::String(s) => Ok(s.into_pyobject(py).unwrap().into_any().unbind()),
        Value::Array(_) | Value::Object(_) => {
            let obj = JsonObj { data: v.clone() };
            Ok(obj.into_pyobject(py).unwrap().into_any().unbind())
        }
    }
}

#[pymethods]
impl JsonObj {
    fn __getattr__(&self, py: Python<'_>, name: &str) -> PyResult<PyObject> {
        // Special case: .value on a primitive returns itself (for Pydantic enum compat)
        if name == "value" {
            match &self.data {
                Value::String(s) => {
                    return Ok(s.into_pyobject(py).unwrap().into_any().unbind())
                }
                Value::Number(n) => {
                    if let Some(i) = n.as_i64() {
                        return Ok(i.into_pyobject(py).unwrap().into_any().unbind());
                    }
                    if let Some(f) = n.as_f64() {
                        return Ok(f.into_pyobject(py).unwrap().into_any().unbind());
                    }
                }
                Value::Bool(b) => {
                    return Ok(b.into_pyobject(py).unwrap().to_owned().into_any().unbind())
                }
                _ => {}
            }
        }

        match &self.data {
            Value::Object(map) => {
                // Try exact match
                if let Some(v) = map.get(name) {
                    // For known enum fields (access, format, resource_type), keep strings
                    // as JsonObj so that .value returns the string (Pydantic enum compat).
                    if matches!(name, "access" | "format" | "resource_type") {
                        if let Value::String(_) = v {
                            return Ok(JsonObj { data: v.clone() }
                                .into_pyobject(py)
                                .unwrap()
                                .into_any()
                                .unbind());
                        }
                    }
                    return value_to_py(py, v);
                }
                // Try without trailing underscore (Pydantic alias: schema_ → schema)
                if name.ends_with('_') {
                    let trimmed = &name[..name.len() - 1];
                    if let Some(v) = map.get(trimmed) {
                        return value_to_py(py, v);
                    }
                }
                Err(PyAttributeError::new_err(format!(
                    "'JsonObj' object has no attribute '{name}'"
                )))
            }
            _ => Err(PyAttributeError::new_err(format!(
                "'JsonObj' object has no attribute '{name}'"
            ))),
        }
    }

    fn __getitem__(&self, py: Python<'_>, key: Py<PyAny>) -> PyResult<PyObject> {
        // Try string key (dict access)
        if let Ok(s) = key.extract::<String>(py) {
            return match &self.data {
                Value::Object(map) => match map.get(&s) {
                    Some(v) => value_to_py(py, v),
                    None => Err(PyKeyError::new_err(s)),
                },
                _ => Err(PyTypeError::new_err("object is not subscriptable")),
            };
        }
        // Try integer key (array access)
        if let Ok(i) = key.extract::<isize>(py) {
            return match &self.data {
                Value::Array(arr) => {
                    let idx = if i < 0 {
                        (arr.len() as isize + i) as usize
                    } else {
                        i as usize
                    };
                    match arr.get(idx) {
                        Some(v) => value_to_py(py, v),
                        None => Err(PyIndexError::new_err("index out of range")),
                    }
                }
                _ => Err(PyTypeError::new_err("object is not subscriptable")),
            };
        }
        Err(PyTypeError::new_err("key must be str or int"))
    }

    fn __len__(&self) -> usize {
        match &self.data {
            Value::Object(map) => map.len(),
            Value::Array(arr) => arr.len(),
            Value::String(s) => s.len(),
            _ => 0,
        }
    }

    fn __iter__(&self, py: Python<'_>) -> PyResult<PyObject> {
        match &self.data {
            Value::Object(map) => {
                let keys: Vec<PyObject> = map
                    .keys()
                    .map(|k| k.into_pyobject(py).unwrap().into_any().unbind())
                    .collect();
                let list = PyList::new(py, &keys).unwrap();
                list.call_method0("__iter__")
                    .map(|o| o.unbind())
            }
            Value::Array(arr) => {
                let items: Vec<PyObject> = arr
                    .iter()
                    .map(|v| value_to_py(py, v))
                    .collect::<PyResult<_>>()?;
                let list = PyList::new(py, &items).unwrap();
                list.call_method0("__iter__")
                    .map(|o| o.unbind())
            }
            _ => Err(PyTypeError::new_err("object is not iterable")),
        }
    }

    fn __contains__(&self, py: Python<'_>, key: Py<PyAny>) -> PyResult<bool> {
        match &self.data {
            Value::Object(map) => {
                if let Ok(s) = key.extract::<String>(py) {
                    Ok(map.contains_key(&s))
                } else {
                    Ok(false)
                }
            }
            Value::Array(arr) => {
                // Check if value is contained in the array
                if let Ok(s) = key.extract::<String>(py) {
                    Ok(arr.iter().any(|v| v.as_str() == Some(&s)))
                } else if let Ok(i) = key.extract::<i64>(py) {
                    Ok(arr.iter().any(|v| v.as_i64() == Some(i)))
                } else if let Ok(f) = key.extract::<f64>(py) {
                    Ok(arr.iter().any(|v| v.as_f64() == Some(f)))
                } else if let Ok(b) = key.extract::<bool>(py) {
                    Ok(arr.iter().any(|v| v.as_bool() == Some(b)))
                } else {
                    Ok(false)
                }
            }
            _ => Ok(false),
        }
    }

    fn __bool__(&self) -> bool {
        match &self.data {
            Value::Null => false,
            Value::Bool(b) => *b,
            Value::Number(n) => n.as_f64().is_some_and(|f| f != 0.0),
            Value::String(s) => !s.is_empty(),
            Value::Array(a) => !a.is_empty(),
            Value::Object(o) => !o.is_empty(),
        }
    }

    fn __repr__(&self) -> String {
        let preview = serde_json::to_string(&self.data).unwrap_or_default();
        if preview.len() > 200 {
            format!("JsonObj({}...)", &preview[..200])
        } else {
            format!("JsonObj({preview})")
        }
    }

    fn __str__(&self) -> String {
        serde_json::to_string(&self.data).unwrap_or_default()
    }

    fn __eq__(&self, py: Python<'_>, other: Py<PyAny>) -> bool {
        if let Ok(s) = other.extract::<String>(py) {
            self.data.as_str() == Some(&s)
        } else if let Ok(i) = other.extract::<i64>(py) {
            self.data.as_i64() == Some(i)
        } else if let Ok(f) = other.extract::<f64>(py) {
            self.data.as_f64() == Some(f)
        } else if let Ok(b) = other.extract::<bool>(py) {
            self.data.as_bool() == Some(b)
        } else if other.is_none(py) {
            self.data.is_null()
        } else {
            false
        }
    }

    /// Dict-like .items() → list of (key, value) tuples.
    fn items(&self, py: Python<'_>) -> PyResult<PyObject> {
        match &self.data {
            Value::Object(map) => {
                let items: Vec<PyObject> = map
                    .iter()
                    .map(|(k, v)| {
                        let py_k: PyObject = k.into_pyobject(py).unwrap().into_any().unbind();
                        let py_v = value_to_py(py, v)?;
                        let tup = PyTuple::new(py, [py_k, py_v]).unwrap();
                        Ok(tup.into_any().unbind())
                    })
                    .collect::<PyResult<_>>()?;
                Ok(PyList::new(py, &items).unwrap().into_any().unbind())
            }
            _ => Err(PyTypeError::new_err("object has no items()")),
        }
    }

    /// Dict-like .keys() → list of keys.
    fn keys(&self, py: Python<'_>) -> PyResult<PyObject> {
        match &self.data {
            Value::Object(map) => {
                let keys: Vec<PyObject> = map
                    .keys()
                    .map(|k| k.into_pyobject(py).unwrap().into_any().unbind())
                    .collect();
                Ok(PyList::new(py, &keys).unwrap().into_any().unbind())
            }
            _ => Err(PyTypeError::new_err("object has no keys()")),
        }
    }

    /// Dict-like .values() → list of values.
    fn values(&self, py: Python<'_>) -> PyResult<PyObject> {
        match &self.data {
            Value::Object(map) => {
                let vals: Vec<PyObject> = map
                    .values()
                    .map(|v| value_to_py(py, v))
                    .collect::<PyResult<_>>()?;
                Ok(PyList::new(py, &vals).unwrap().into_any().unbind())
            }
            _ => Err(PyTypeError::new_err("object has no values()")),
        }
    }

    /// Dict-like .get(key, default=None).
    #[pyo3(signature = (key, default=None))]
    fn get(&self, py: Python<'_>, key: &str, default: Option<PyObject>) -> PyResult<PyObject> {
        match &self.data {
            Value::Object(map) => match map.get(key) {
                Some(v) => value_to_py(py, v),
                None => Ok(default.unwrap_or_else(|| py.None())),
            },
            _ => Ok(default.unwrap_or_else(|| py.None())),
        }
    }

    /// Convert the entire JsonObj tree to native Python types (dict/list/str/int/float/bool/None).
    fn to_python(&self, py: Python<'_>) -> PyResult<PyObject> {
        self.deep_convert(py)
    }
}

impl JsonObj {
    /// Recursively convert to native Python types.
    fn deep_convert(&self, py: Python<'_>) -> PyResult<PyObject> {
        match &self.data {
            Value::Null => Ok(py.None()),
            Value::Bool(b) => Ok(b.into_pyobject(py).unwrap().to_owned().into_any().unbind()),
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Ok(i.into_pyobject(py).unwrap().into_any().unbind())
                } else if let Some(f) = n.as_f64() {
                    Ok(f.into_pyobject(py).unwrap().into_any().unbind())
                } else {
                    Ok(py.None())
                }
            }
            Value::String(s) => Ok(s.into_pyobject(py).unwrap().into_any().unbind()),
            Value::Array(arr) => {
                let items: Vec<PyObject> = arr
                    .iter()
                    .map(|v| JsonObj { data: v.clone() }.deep_convert(py))
                    .collect::<PyResult<_>>()?;
                Ok(PyList::new(py, &items).unwrap().into_any().unbind())
            }
            Value::Object(map) => {
                let dict = pyo3::types::PyDict::new(py);
                for (k, v) in map {
                    dict.set_item(k, JsonObj { data: v.clone() }.deep_convert(py)?)?;
                }
                Ok(dict.into_any().unbind())
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Top-level parse functions
// ---------------------------------------------------------------------------

/// Parse a dbt manifest JSON string into a JsonObj.
#[pyfunction]
fn parse_manifest_json(py: Python<'_>, json_str: &str) -> PyResult<PyObject> {
    let data: Value = serde_json::from_str(json_str)
        .map_err(|e| PyValueError::new_err(format!("Invalid manifest JSON: {e}")))?;
    Ok(JsonObj { data }.into_pyobject(py).unwrap().into_any().unbind())
}

/// Parse a dbt catalog JSON string into a JsonObj.
#[pyfunction]
fn parse_catalog_json(py: Python<'_>, json_str: &str) -> PyResult<PyObject> {
    let data: Value = serde_json::from_str(json_str)
        .map_err(|e| PyValueError::new_err(format!("Invalid catalog JSON: {e}")))?;
    Ok(JsonObj { data }.into_pyobject(py).unwrap().into_any().unbind())
}

/// Parse a dbt run-results JSON string into a JsonObj.
#[pyfunction]
fn parse_run_results_json(py: Python<'_>, json_str: &str) -> PyResult<PyObject> {
    let data: Value = serde_json::from_str(json_str)
        .map_err(|e| PyValueError::new_err(format!("Invalid run_results JSON: {e}")))?;
    Ok(JsonObj { data }.into_pyobject(py).unwrap().into_any().unbind())
}

// ---------------------------------------------------------------------------
// Python module
// ---------------------------------------------------------------------------

#[pymodule]
fn dbt_artifacts_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<JsonObj>()?;
    m.add_function(wrap_pyfunction!(parse_manifest_json, m)?)?;
    m.add_function(wrap_pyfunction!(parse_catalog_json, m)?)?;
    m.add_function(wrap_pyfunction!(parse_run_results_json, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // --- Parse functions ---

    #[test]
    fn test_parse_manifest_json_valid() {
        Python::with_gil(|py| {
            let json_str = r#"{"metadata": {"dbt_version": "1.7.0"}, "nodes": {}}"#;
            let result = parse_manifest_json(py, json_str);
            assert!(result.is_ok());
        });
    }

    #[test]
    fn test_parse_manifest_json_invalid() {
        Python::with_gil(|py| {
            let result = parse_manifest_json(py, "not json");
            assert!(result.is_err());
        });
    }

    #[test]
    fn test_parse_catalog_json_valid() {
        Python::with_gil(|py| {
            let json_str = r#"{"metadata": {}, "nodes": {}, "sources": {}}"#;
            let result = parse_catalog_json(py, json_str);
            assert!(result.is_ok());
        });
    }

    #[test]
    fn test_parse_catalog_json_invalid() {
        Python::with_gil(|py| {
            let result = parse_catalog_json(py, "{broken");
            assert!(result.is_err());
        });
    }

    #[test]
    fn test_parse_run_results_json_valid() {
        Python::with_gil(|py| {
            let json_str = r#"{"metadata": {}, "results": []}"#;
            let result = parse_run_results_json(py, json_str);
            assert!(result.is_ok());
        });
    }

    #[test]
    fn test_parse_run_results_json_invalid() {
        Python::with_gil(|py| {
            let result = parse_run_results_json(py, "");
            assert!(result.is_err());
        });
    }

    // --- JsonObj attribute access ---

    #[test]
    fn test_getattr_exact_match() {
        Python::with_gil(|py| {
            let obj = JsonObj {
                data: json!({"name": "my_model"}),
            };
            let result = obj.__getattr__(py, "name").unwrap();
            let s: String = result.extract(py).unwrap();
            assert_eq!(s, "my_model");
        });
    }

    #[test]
    fn test_getattr_missing_key() {
        Python::with_gil(|py| {
            let obj = JsonObj {
                data: json!({"name": "my_model"}),
            };
            let result = obj.__getattr__(py, "missing");
            assert!(result.is_err());
        });
    }

    #[test]
    fn test_getattr_trailing_underscore() {
        Python::with_gil(|py| {
            let obj = JsonObj {
                data: json!({"schema": "public"}),
            };
            let result = obj.__getattr__(py, "schema_").unwrap();
            let s: String = result.extract(py).unwrap();
            assert_eq!(s, "public");
        });
    }

    #[test]
    fn test_getattr_enum_field_returns_jsonobj() {
        Python::with_gil(|py| {
            let obj = JsonObj {
                data: json!({"access": "public"}),
            };
            let result = obj.__getattr__(py, "access").unwrap();
            // Should be a JsonObj, not a plain string
            let is_string: Result<String, _> = result.extract(py);
            assert!(
                is_string.is_err(),
                "enum fields should return JsonObj, not plain string"
            );
        });
    }

    #[test]
    fn test_getattr_value_on_primitive() {
        Python::with_gil(|py| {
            let obj = JsonObj {
                data: json!("hello"),
            };
            let result = obj.__getattr__(py, "value").unwrap();
            let s: String = result.extract(py).unwrap();
            assert_eq!(s, "hello");
        });
    }

    // --- JsonObj dict operations ---

    #[test]
    fn test_getitem_string_key() {
        Python::with_gil(|py| {
            let obj = JsonObj {
                data: json!({"key": "val"}),
            };
            let key = "key".into_pyobject(py).unwrap().into_any().unbind();
            let result = obj.__getitem__(py, key).unwrap();
            let s: String = result.extract(py).unwrap();
            assert_eq!(s, "val");
        });
    }

    #[test]
    fn test_getitem_missing_key() {
        Python::with_gil(|py| {
            let obj = JsonObj {
                data: json!({"key": "val"}),
            };
            let key = "missing".into_pyobject(py).unwrap().into_any().unbind();
            let result = obj.__getitem__(py, key);
            assert!(result.is_err());
        });
    }

    #[test]
    fn test_getitem_array_index() {
        Python::with_gil(|py| {
            let obj = JsonObj {
                data: json!([10, 20, 30]),
            };
            let key = 1i64.into_pyobject(py).unwrap().into_any().unbind();
            let result = obj.__getitem__(py, key).unwrap();
            let i: i64 = result.extract(py).unwrap();
            assert_eq!(i, 20);
        });
    }

    #[test]
    fn test_getitem_negative_index() {
        Python::with_gil(|py| {
            let obj = JsonObj {
                data: json!([10, 20, 30]),
            };
            let key = (-1i64).into_pyobject(py).unwrap().into_any().unbind();
            let result = obj.__getitem__(py, key).unwrap();
            let i: i64 = result.extract(py).unwrap();
            assert_eq!(i, 30);
        });
    }

    #[test]
    fn test_len_object() {
        let obj = JsonObj {
            data: json!({"a": 1, "b": 2}),
        };
        assert_eq!(obj.__len__(), 2);
    }

    #[test]
    fn test_len_array() {
        let obj = JsonObj {
            data: json!([1, 2, 3]),
        };
        assert_eq!(obj.__len__(), 3);
    }

    #[test]
    fn test_contains_object_key() {
        Python::with_gil(|py| {
            let obj = JsonObj {
                data: json!({"name": "x"}),
            };
            let key = "name".into_pyobject(py).unwrap().into_any().unbind();
            assert!(obj.__contains__(py, key).unwrap());
        });
    }

    #[test]
    fn test_contains_array_value() {
        Python::with_gil(|py| {
            let obj = JsonObj {
                data: json!(["a", "b", "c"]),
            };
            let key = "b".into_pyobject(py).unwrap().into_any().unbind();
            assert!(obj.__contains__(py, key).unwrap());
        });
    }

    #[test]
    fn test_contains_array_missing() {
        Python::with_gil(|py| {
            let obj = JsonObj {
                data: json!(["a", "b"]),
            };
            let key = "z".into_pyobject(py).unwrap().into_any().unbind();
            assert!(!obj.__contains__(py, key).unwrap());
        });
    }

    // --- Primitives ---

    #[test]
    fn test_bool_truthy() {
        assert!(JsonObj { data: json!(true) }.__bool__());
        assert!(JsonObj { data: json!(1) }.__bool__());
        assert!(JsonObj {
            data: json!("hello")
        }
        .__bool__());
        assert!(JsonObj { data: json!([1]) }.__bool__());
        assert!(JsonObj {
            data: json!({"a": 1})
        }
        .__bool__());
    }

    #[test]
    fn test_bool_falsy() {
        assert!(!JsonObj { data: json!(false) }.__bool__());
        assert!(!JsonObj { data: json!(null) }.__bool__());
        assert!(!JsonObj { data: json!(0) }.__bool__());
        assert!(!JsonObj { data: json!("") }.__bool__());
        assert!(!JsonObj { data: json!([]) }.__bool__());
        assert!(!JsonObj {
            data: json!({})
        }
        .__bool__());
    }

    #[test]
    fn test_eq_string() {
        Python::with_gil(|py| {
            let obj = JsonObj {
                data: json!("hello"),
            };
            let other = "hello".into_pyobject(py).unwrap().into_any().unbind();
            assert!(obj.__eq__(py, other));
        });
    }

    #[test]
    fn test_eq_int() {
        Python::with_gil(|py| {
            let obj = JsonObj { data: json!(42) };
            let other = 42i64.into_pyobject(py).unwrap().into_any().unbind();
            assert!(obj.__eq__(py, other));
        });
    }

    #[test]
    fn test_eq_null_none() {
        Python::with_gil(|py| {
            let obj = JsonObj { data: json!(null) };
            assert!(obj.__eq__(py, py.None()));
        });
    }

    #[test]
    fn test_repr_short() {
        let obj = JsonObj {
            data: json!({"a": 1}),
        };
        let r = obj.__repr__();
        assert!(r.starts_with("JsonObj("));
        assert!(r.contains("\"a\""));
    }

    #[test]
    fn test_str() {
        let obj = JsonObj { data: json!(42) };
        assert_eq!(obj.__str__(), "42");
    }

    // --- Dict-like methods ---

    #[test]
    fn test_get_existing_key() {
        Python::with_gil(|py| {
            let obj = JsonObj {
                data: json!({"x": 10}),
            };
            let result = obj.get(py, "x", None).unwrap();
            let i: i64 = result.extract(py).unwrap();
            assert_eq!(i, 10);
        });
    }

    #[test]
    fn test_get_missing_key_returns_default() {
        Python::with_gil(|py| {
            let obj = JsonObj {
                data: json!({"x": 10}),
            };
            let result = obj.get(py, "y", None).unwrap();
            assert!(result.is_none(py));
        });
    }

    #[test]
    fn test_keys() {
        Python::with_gil(|py| {
            let obj = JsonObj {
                data: json!({"a": 1, "b": 2}),
            };
            let result = obj.keys(py).unwrap();
            let list: Vec<String> = result.extract(py).unwrap();
            assert!(list.contains(&"a".to_string()));
            assert!(list.contains(&"b".to_string()));
            assert_eq!(list.len(), 2);
        });
    }

    #[test]
    fn test_keys_on_non_object() {
        Python::with_gil(|py| {
            let obj = JsonObj {
                data: json!([1, 2]),
            };
            let result = obj.keys(py);
            assert!(result.is_err());
        });
    }
}
