use crate::python::py_task_definition::DFRayTaskDefinition;
use futures_util::{StreamExt, TryStreamExt};
use pyo3::{pyfunction, PyResult, Python};
use std::sync::Arc;
use datafusion_python::utils::wait_for_future;
use crate::run::{get_tasks_inner, run};
use crate::streaming::coordinator::PythonResources;
use pyo3::prelude::*;

#[pyfunction]
pub fn entrypoint(
    py: Python,
    processor_class: PyObject,
    ray_get: PyObject,
) -> PyResult<()> {
    let python_resources = Arc::new(PythonResources::new(processor_class, ray_get)?);
    wait_for_future(py, run(python_resources))
}

#[pyfunction]
pub fn get_tasks(py: Python) -> PyResult<Vec<DFRayTaskDefinition>> {
    Ok(get_tasks_inner()
        .into_iter()
        .map(|task| {
            DFRayTaskDefinition {
                task_definition2: task,
            }
        })
        .collect())
}
