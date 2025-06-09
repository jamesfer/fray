use std::borrow::Cow;
use pyo3::{pyclass, pymethods, PyResult, Python};
use crate::streaming::task_definition_2::TaskDefinition2;

#[pyclass]
#[derive(Clone)]
pub struct DFRayTaskDefinition {
    pub task_definition2: TaskDefinition2,
}

#[pymethods]
impl DFRayTaskDefinition {
    pub fn task_bytes(&self, py: Python) -> PyResult<Cow<[u8]>> {
        match flexbuffers::to_vec(&self.task_definition2) {
            Ok(bytes) => Ok(Cow::Owned(bytes)),
            Err(e) => Err(pyo3::exceptions::PyValueError::new_err(format!(
                "Failed to serialize TaskDefinition2 to Flexbuffer: {}",
                e
            ))),
        }
    }

    pub fn output_streams(&self) -> Vec<String> {
        self.task_definition2.exchange_outputs()
    }
}
