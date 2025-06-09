use std::borrow::Cow;
use pyo3::{pyclass, pymethods, PyResult, Python};
use crate::streaming::worker_process::InitialSchedulingDetails;

#[pyclass]
#[derive(Clone)]
pub struct DFRayInitialSchedulingDetails {
    pub initial_scheduling_details: InitialSchedulingDetails,
}

#[pymethods]
impl DFRayInitialSchedulingDetails {
    pub fn bytes(&self, py: Python) -> PyResult<Cow<[u8]>> {
        match flexbuffers::to_vec(&self.initial_scheduling_details) {
            Ok(bytes) => Ok(Cow::Owned(bytes)),
            Err(e) => Err(pyo3::exceptions::PyValueError::new_err(format!(
                "Failed to serialize DFRayInitialSchedulingDetails to Flexbuffer: {}",
                e
            ))),
        }
    }
}
