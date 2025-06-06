use std::borrow::Cow;
use std::time::Duration;
use datafusion_python::utils::wait_for_future;
use futures_util::{StreamExt, TryStreamExt};
use pyo3::{pyclass, pyfunction, pymethods, PyResult, Python};
use tokio::time::sleep;
use datafusion::common::record_batch;
use crate::dataframe::PyRecordBatch;
use crate::streaming::generation::{GenerationInputDetail, GenerationInputLocation, GenerationSpec};
use crate::streaming::operators::nested::NestedOperator;
use crate::streaming::operators::operator::{OperatorDefinition, OperatorInput, OperatorOutput, OperatorSpec};
use crate::streaming::operators::remote_exchange::RemoteExchangeOperator;
use crate::streaming::operators::source::SourceOperator;
use crate::streaming::operators::task_function::SItem;
use crate::streaming::task_definition_2::TaskDefinition2;
use crate::streaming::utils::create_remote_stream::{create_remote_stream, create_remote_stream_no_runtime};
use crate::streaming::worker_process::InitialSchedulingDetails;

#[pyclass]
#[derive(Clone)]
pub struct DFRayTaskDefinition {
    task_definition2: TaskDefinition2,
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

// #[pyclass]
// #[derive(Clone)]
// pub struct DFRayGenerationSpec {
//     generation_spec: GenerationSpec,
// }
//
// #[pymethods]
// impl DFRayGenerationSpec {
//     #[new]
//     pub fn new(id: String, partitions: Vec<usize>) -> Self {
//         DFRayGenerationSpec {
//             generation_spec: GenerationSpec {
//                 id,
//                 partitions,
//                 start_conditions: vec![],
//             },
//         }
//     }
// }

// #[pyclass]
// pub struct DFRayInputDetail {
//     input_detail: GenerationInputDetail,
// }
//
// #[pymethods]
// impl DFRayInputDetail {
//     #[new]
//     pub fn new(stream_id: String, locations: Vec<(String, (usize, usize), Vec<usize>)>) -> Self {
//         let locations = locations.into_iter()
//             .map(|(address, offset_range, partitions)| GenerationInputLocation {
//                 address,
//                 offset_range,
//                 partitions,
//             })
//             .collect();
//         DFRayInputDetail {
//             input_detail: GenerationInputDetail {
//                 stream_id,
//                 locations,
//             },
//         }
//     }
// }

#[pyclass]
#[derive(Clone)]
pub struct DFRayInitialSchedulingDetails {
    initial_scheduling_details: InitialSchedulingDetails,
}

#[pymethods]
impl DFRayInitialSchedulingDetails {
    // #[new]
    // pub fn new(input_details: Vec<DFRayInputDetail>, initial_generation: DFRayGenerationSpec) -> Self {
    //     DFRayInitialSchedulingDetails {
    //         initial_scheduling_details: InitialSchedulingDetails {
    //             input_locations: input_details.into_iter()
    //                 .map(|detail| detail.input_detail)
    //                 .collect(),
    //             generations: vec![initial_generation.generation_spec],
    //         },
    //     }
    // }

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

#[pyfunction]
pub fn get_tasks(py: Python) -> PyResult<Vec<DFRayTaskDefinition>> {
    let test_batch = record_batch!(
        ("a", Int32, vec![1i32, 2, 3])
    ).unwrap();
    let task_definition = TaskDefinition2 {
        task_id: "task1".to_string(),
        operator: OperatorDefinition {
            id: "nested1".to_string(),
            state_id: "1".to_string(),
            spec: OperatorSpec::Nested(NestedOperator::new(
                vec![],
                vec![
                    OperatorDefinition {
                        id: "source1".to_string(),
                        state_id: "2".to_string(),
                        spec: OperatorSpec::Source(SourceOperator::new(vec![test_batch.clone()])),
                        inputs: vec![],
                        outputs: vec![OperatorOutput {
                            stream_id: "output1".to_string(),
                            ordinal: 0,
                        }],
                    },
                    OperatorDefinition {
                        id: "exchange1".to_string(),
                        state_id: "3".to_string(),
                        spec: OperatorSpec::RemoteExchangeOutput(RemoteExchangeOperator::new("exchange_output1".to_string())),
                        inputs: vec![OperatorInput {
                            stream_id: "output1".to_string(),
                            ordinal: 0,
                        }],
                        outputs: vec![],
                    },
                ],
                vec![],
            )),
            inputs: vec![],
            outputs: vec![],
        },
    };

    Ok(vec![DFRayTaskDefinition {
        task_definition2: task_definition,
    }])
}


async fn retry_future<F, Fut, T, E>(mut retries: u32, mut f: F) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    loop {
        match f().await {
            Ok(res) => return Ok(res),
            Err(e) => {
                if retries == 0 {
                    return Err(e);
                }
                eprintln!("Operation failed, retrying... ({}/{}). Error: {}", retries, 5, e);
                retries -= 1;
                sleep(Duration::from_millis(100)).await;
            }
        }
    }
}


// #[cfg(test)]
// mod tests {
//     use pyo3::{Bound, PyResult, Python};
//     use pyo3::types::{PyAnyMethods, PyModule};
//
//     #[test]
//     fn go() {
//         Python::with_gil(|py| {
//             let module = module(py).expect("Failed to create module");
//
//             let house_class = module.getattr("House").unwrap();
//             let house = house_class.call1(("123 Main Street",)).unwrap();
//
//             house.call_method0("__enter__").unwrap();
//
//             let result = py.eval(c"1 + 1", None, None);
//
//             // If the eval threw an exception we'll pass it through to the context manager.
//             // Otherwise, __exit__  is called with empty arguments (Python "None").
//             match result {
//                 Ok(_) => {
//                     let none = py.None();
//                     house.call_method1("__exit__", (&none, &none, &none)).unwrap();
//                 },
//                 Err(e) => {
//                     house.call_method1("__exit__", (e.get_type(py), e.value(py), e.traceback(py))).unwrap();
//                 }
//             }
//         });
//     }
//
//     fn module(py: Python) -> PyResult<Bound<PyModule>> {
//         PyModule::from_code(
//             py,
//             c"
//                 #import ray
//
//                 class House(object):
//                     def __init__(self, address):
//                         self.address = address
//                     def __enter__(self):
//                         print(f\"Welcome to {self.address}!\")
//                     def __exit__(self, type, value, traceback):
//                         if type:
//                             print(f\"Sorry you had {type} trouble at {self.address}\")
//                         else:
//                             print(f\"Thank you for visiting {self.address}, come again soon!\")
//             ",
//             c"house.py",
//             c"house",
//         )
//     }
// }
