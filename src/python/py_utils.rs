use crate::dataframe::PyRecordBatch;
use crate::python::py_scheduling_details::DFRayInitialSchedulingDetails;
use crate::python::py_task_definition::DFRayTaskDefinition;
use crate::streaming::generation::{GenerationInputDetail, GenerationInputLocation, GenerationSpec};
use crate::streaming::operators::task_function::SItem;
use crate::streaming::utils::create_remote_stream::create_remote_stream_no_runtime;
use crate::streaming::utils::retry::retry_future;
use crate::streaming::worker_process::InitialSchedulingDetails;
use datafusion_python::utils::wait_for_future;
use futures_util::TryStreamExt;
use pyo3::{pyfunction, PyResult, Python};

#[pyfunction]
pub fn collect(py: Python, address: String, stream_id: String) -> PyResult<Vec<PyRecordBatch>> {
    wait_for_future(py, async move {
        let stream = retry_future(5, || {
            create_remote_stream_no_runtime(
                &stream_id,
                &address,
                vec![0],
            )
        }).await
            .map_err(|e| {;
                pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "Failed to create remote stream: {}",
                    e
                ))
            })?;

        let results = Box::into_pin(stream).try_collect::<Vec<_>>().await
            .map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "Failed to collect from remote stream: {}",
                    e
                ))
            })?;
        Ok(results.into_iter()
            .filter_map(|item| {
                match item {
                    SItem::RecordBatch(record_batch) => Some(record_batch),
                    _ => None,
                }
            })
            .map(|record_batch| {
                PyRecordBatch::from(record_batch)
            })
            .collect::<Vec<PyRecordBatch>>())
    })
}

#[pyfunction]
pub fn schedule_without_partitions(py: Python, assigned_tasks: Vec<(DFRayTaskDefinition, String)>) -> DFRayInitialSchedulingDetails {
    let initial_generation = GenerationSpec {
        id: "initial_generation".to_string(),
        partitions: vec![0],
        start_conditions: vec![],
    };
    let input_details = assigned_tasks.iter()
        .flat_map(|(task, address)| {
            task.task_definition2.exchange_outputs()
                .into_iter()
                .map(|stream_id| {
                    GenerationInputDetail {
                        stream_id,
                        locations: vec![GenerationInputLocation {
                            address: address.clone(),
                            offset_range: (0, 2 << 31),
                            partitions: vec![0],
                        }],
                    }
                })
        })
        .collect::<Vec<_>>();
    let initial_scheduling_details = DFRayInitialSchedulingDetails {
        initial_scheduling_details: InitialSchedulingDetails {
            input_locations: input_details,
            generations: vec![initial_generation],
        },
    };
    initial_scheduling_details
}
