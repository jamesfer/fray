use crate::streaming::coordinator::PythonResources;
use crate::streaming::task_definition_2::TaskDefinition2;
use crate::streaming::worker_process::InitialSchedulingDetails;
use pyo3::prelude::PyAnyMethods;
use pyo3::{Bound, Py, PyAny, PyResult, Python};
use std::sync::Arc;

// The rust version of the python Processor class
pub struct RemoteProcessor {
    processor_object: Py<PyAny>,
    python_pool: Arc<PythonResources>,
    addr: String,
}

impl RemoteProcessor {
    pub async fn start(
        python_pool: Arc<PythonResources>,
    ) -> PyResult<Self> {
        let pool = python_pool.clone();
        let (processor_object, addr) = pool.with_python(|processor_class, ray_get| {
            let processor_object = Self::init_remote(processor_class)?;
            let addr = ray_get.call1((Self::start_up_remote(&processor_object)?, ))?
                .extract::<String>()?;
            
            Ok((processor_object.unbind(), addr))
        }).await?;

        Ok(RemoteProcessor {
            processor_object,
            python_pool,
            addr,
        })
    }

    pub async fn start_many(
        python_pool: Arc<PythonResources>,
        num_processors: usize,
    ) -> PyResult<Vec<Self>> {
        let pool = python_pool.clone();
        let processors = pool.with_python(|processor_class, ray_get| {
            // Initialize all processors
            let processor_objects = (0..num_processors)
                .map(|_| Self::init_remote(processor_class))
                .collect::<PyResult<Vec<_>>>()?;

            // Wait for them to be ready
            let addr = ray_get.call1((
                processor_objects.iter()
                    .map(|processor| Self::start_up_remote(processor))
                    .collect::<PyResult<Vec<_>>>()?,
            ))?.extract::<Vec<String>>()?;

            let processors = processor_objects.into_iter()
                .zip(addr.into_iter())
                .map(|(processor_object, addr)| Self::new(processor_object.unbind(), python_pool.clone(), addr))
                .collect();
            Ok(processors)
        }).await?;
        
        Ok(processors)
    }

    pub fn addr(&self) -> &str {
        &self.addr
    }

    fn init_remote(processor_class: &Bound<PyAny>) -> PyResult<Bound<PyAny>> {
        processor_class.getattr("remote")?.call0()
    }

    fn start_up_remote(processor: &Bound<PyAny>) -> PyResult<Bound<PyAny>> {
        processor.getattr("start_up")?.getattr("remote")?.call0()
    }

    fn new(
        processor_object: Py<PyAny>,
        python_pool: Arc<PythonResources>,
        addr: String,
    ) -> Self {
        RemoteProcessor {
            processor_object,
            python_pool,
            addr,
        }
    }

    async fn start_up(&self) -> PyResult<String> {
        let processor_object = Python::with_gil(|py| self.processor_object.clone_ref(py));
        tokio::task::spawn_blocking(move || {
            Python::with_gil(|py| -> PyResult<String> {
                let processor_bound = processor_object.bind(py);
                processor_bound.getattr("start_up")?.call0()?.extract()
            })
        }).await.unwrap()
    }

    pub async fn update_plan(
        &self,
        task_definition2: &TaskDefinition2,
        initial_scheduling_details: &InitialSchedulingDetails,
    ) -> PyResult<()> {
        let task_bytes = task_definition2.to_bytes()
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!(
                "Failed to serialize TaskDefinition2: {}",
                e
            )))?;
        let detail_bytes = initial_scheduling_details.to_bytes()
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!(
                "Failed to serialize InitialSchedulingDetails: {}",
                e
            )))?;
        
        self.python_pool.with_python(|_processor_class, _ray_get| {
            Python::with_gil(|py| {
                self.processor_object.bind(py).getattr("update_plan")?
                    .getattr("remote")?
                    .call1((task_bytes, detail_bytes))?;
                Ok(())
            })
        }).await
    }
}
