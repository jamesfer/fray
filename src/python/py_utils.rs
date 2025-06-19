use arrow_array::RecordBatch;
use crate::dataframe::PyRecordBatch;
use crate::python::py_scheduling_details::DFRayInitialSchedulingDetails;
use crate::python::py_task_definition::DFRayTaskDefinition;
use crate::streaming::generation::{GenerationInputDetail, GenerationInputLocation, GenerationSpec};
use crate::streaming::operators::task_function::SItem;
use crate::streaming::partitioning::PartitionRange;
use crate::streaming::utils::create_remote_stream::create_remote_stream_no_runtime;
use crate::streaming::utils::retry::retry_future;
use crate::streaming::worker_process::InitialSchedulingDetails;
use datafusion_python::utils::wait_for_future;
use futures::{Stream, StreamExt};
use futures::TryStreamExt;
use pyo3::{pyfunction, PyErr, PyResult, Python};
use datafusion::error::DataFusionError;
use crate::streaming::task_definition_2::TaskDefinition2;

#[pyfunction]
pub fn collect(py: Python, address: String, stream_id: String) -> PyResult<Vec<PyRecordBatch>> {
    Ok(
        wait_for_future(py, async move {
            collect_inner(&address, &stream_id).await
        })
            .map_err(|err| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                    format!("Failed to collect record batches: {}", err)
                )
            })?
            .into_iter()
            .map(PyRecordBatch::from)
            .collect()
    )
}

pub async fn collect_inner(address: &str, stream_id: &str) -> Result<Vec<RecordBatch>, DataFusionError> {
    stream_results(address, stream_id).await?.try_collect::<Vec<_>>().await
}

pub async fn stream_results(address: &str, stream_id: &str) -> Result<impl Stream<Item=Result<RecordBatch, DataFusionError>> + use<>, DataFusionError> {
    let stream = retry_future(5, || {
        create_remote_stream_no_runtime(
            &stream_id,
            &address,
            PartitionRange::empty(),
        )
    }).await?;
    let stream = Box::into_pin(stream);
    let stream = stream.map(|result| {
        // Use a match statement to print each value of result
        match result {
            Ok(SItem::RecordBatch(record_batch)) => {
                println!("Collect (py utils) Received record batch: {:?}", record_batch);
                Ok(SItem::RecordBatch(record_batch))
            },
            Ok(SItem::Marker(marker)) => {
                println!("Collect (py utils) Received marker: {}", marker.checkpoint_number);
                Ok(SItem::Marker(marker))
            },
            Ok(SItem::Generation(usize)) => {
                println!("Collect (py utils) Received generation item");
                Ok(SItem::Generation(usize))
            },
            Err(err) => {
                println!("Collect (py utils) Error in stream: {}", err);
                Err(err)
            },
        }
    });

    let results = stream
        .filter_map(|item| async move {
            match item {
                Err(e) => Some(Err(e)),
                Ok(SItem::RecordBatch(record_batch)) => Some(Ok(record_batch)),
                _ => None,
            }
        });
    Ok(results)
}

#[pyfunction]
pub fn schedule_without_partitions(py: Python, assigned_tasks: Vec<(DFRayTaskDefinition, String)>) -> DFRayInitialSchedulingDetails {
    let inner_tasks = assigned_tasks.iter()
        .map(|(task, address)| (&task.task_definition2, address.clone()))
        .collect::<Vec<_>>();
    DFRayInitialSchedulingDetails {
        initial_scheduling_details: schedule_without_partitions_inner(&inner_tasks),
    }
}

pub fn schedule_without_partitions_inner(assigned_tasks: &[(&TaskDefinition2, String)]) -> InitialSchedulingDetails {
    let initial_generation = GenerationSpec {
        id: "initial_generation".to_string(),
        partitions: PartitionRange::empty(),
        start_conditions: vec![],
    };
    let input_details = assigned_tasks.iter()
        .flat_map(|(task, address)| {
            task.exchange_outputs()
                .into_iter()
                .map(|stream_id| {
                    GenerationInputDetail {
                        stream_id,
                        locations: vec![GenerationInputLocation {
                            address: address.clone(),
                            offset_range: (0, 2 << 31),
                            partitions: PartitionRange::empty(),
                        }],
                    }
                })
        })
        .collect::<Vec<_>>();
    InitialSchedulingDetails {
        input_locations: input_details,
        generations: vec![initial_generation],
    }
}

pub fn schedule_with_constant_partitions(assigned_tasks: &[(&TaskDefinition2, String)], partition: PartitionRange) -> InitialSchedulingDetails {
    let initial_generation = GenerationSpec {
        id: "initial_generation".to_string(),
        partitions: partition.clone(),
        start_conditions: vec![],
    };
    let input_details = assigned_tasks.iter()
        .flat_map(|(task, address)| {
            task.exchange_outputs()
                .into_iter()
                .map(|stream_id| {
                    GenerationInputDetail {
                        stream_id,
                        locations: vec![GenerationInputLocation {
                            address: address.clone(),
                            offset_range: (0, 2 << 31),
                            partitions: partition.clone(),
                        }],
                    }
                })
        })
        .collect::<Vec<_>>();
    InitialSchedulingDetails {
        input_locations: input_details,
        generations: vec![initial_generation],
    }
}
