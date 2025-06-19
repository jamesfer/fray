use std::sync::Arc;
use arrow_array::RecordBatch;
use futures::Stream;
use pyo3::{PyObject, Python, Py, PyAny, Bound, PyResult};
use crate::python::py_utils::{schedule_with_constant_partitions, schedule_without_partitions_inner, stream_results};
use crate::python::remote_processor::RemoteProcessor;
use crate::streaming::generation::{GenerationInputDetail, GenerationInputLocation, GenerationSpec};
use crate::streaming::partitioning::PartitionRange;
use crate::streaming::state::checkpoint_storage::FileSystemStateStorage;
use crate::streaming::task_definition_2::TaskDefinition2;
use crate::streaming::worker_process::InitialSchedulingDetails;
use futures::stream::{StreamExt, TryStreamExt};
use futures_util::FutureExt;
use futures_util::stream::FuturesOrdered;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use datafusion::common::internal_datafusion_err;
use datafusion::error::DataFusionError;
use crate::streaming::state::file_system::PrefixedLocalFileSystemStorage;

pub struct PythonResources {
    processor_class: Py<PyAny>,
    ray_get: Py<PyAny>,
}

impl PythonResources {
    pub fn new(processor_class: PyObject, ray_get: PyObject) -> PyResult<Self> {
        Python::with_gil(|py| {
            Ok(Self {
                processor_class: processor_class.bind(py).clone().unbind(),
                ray_get: ray_get.bind(py).clone().unbind(),
            })
        })
    }
    
    pub async fn with_python<F, R>(&self, f: F) -> PyResult<R>
    where
        F: FnOnce(&Bound<PyAny>, &Bound<PyAny>) -> PyResult<R> + Send + 'static,
        R: Send + 'static,
    {
        let processor_class = Python::with_gil(|py| self.processor_class.clone_ref(py));
        let ray_get = Python::with_gil(|py| self.ray_get.clone_ref(py));
        
        tokio::task::spawn_blocking(move || {
            Python::with_gil(|py| {
                let pc_bound = processor_class.bind(py);
                let rg_bound = ray_get.bind(py);
                f(&pc_bound, &rg_bound)
            })
        }).await.unwrap()
    }
}

pub struct ComputeEnv {
    python_resources: Arc<PythonResources>,
}

impl ComputeEnv {
    pub fn new(python_resources: Arc<PythonResources>) -> Self {
        ComputeEnv { python_resources }
    }

    pub async fn start_processor_and_run(
        &self,
        task_definition: TaskDefinition2,
        initial_scheduling_details: InitialSchedulingDetails,
    ) -> PyResult<RemoteProcessor> {
        let processor = RemoteProcessor::start(self.python_resources.clone(), None).await?;
        processor.update_plan(&task_definition, &initial_scheduling_details).await?;
        Ok(processor)
    }

    pub async fn start_many_processors(
        &self,
        num_processors: usize,
        remote_checkpoint_dir: Option<String>,
    ) -> PyResult<Vec<RemoteProcessor>> {
        RemoteProcessor::start_many(self.python_resources.clone(), num_processors, remote_checkpoint_dir).await
    }
}

pub struct StaticCoordinator {
    compute_env: Arc<ComputeEnv>,
}

impl StaticCoordinator {
    pub fn new(compute_env: Arc<ComputeEnv>) -> Self {
        Self { compute_env }
    }

    pub async fn schedule_together<'a>(
        &'_ self,
        tasks: &'a [TaskDefinition2],
        remote_checkpoint_dir: Option<String>,
    ) -> PyResult<Vec<(&'a TaskDefinition2, String, RemoteProcessor)>> {
        let processors = self.compute_env.start_many_processors(tasks.len(), remote_checkpoint_dir).await?;
        println!("Processors started: {}", processors.len());

        let assigned_tasks = tasks.into_iter()
            .zip(processors.iter().map(|processor| processor.addr().to_string()))
            .collect::<Vec<_>>();
        let scheduling_details = schedule_without_partitions_inner(&assigned_tasks);

        // Start the tasks in the background
        for ((task, addr), processor) in assigned_tasks.iter().zip(processors.iter()) {
            println!("Assigning task {} to processor {}", task.task_id, addr);
            processor.update_plan(task, &scheduling_details).await?;
        }

        println!("Tasks deployed");

        Ok(assigned_tasks.into_iter()
            .zip(processors.into_iter())
            .map(|((task, addr), processor)| (task, addr, processor))
            .collect())
    }
}

pub struct ActiveCoordinator {
    compute_env: Arc<ComputeEnv>,
    remote_checkpoint_storage: Arc<FileSystemStateStorage>,
    remote_checkpoint_dir: String,
    tasks: Vec<(TaskDefinition2, Vec<(String, RemoteProcessor, PartitionRange)>)>,
}

impl ActiveCoordinator {
    pub async fn start_single_copies(
        compute_env: Arc<ComputeEnv>,
        tasks: Vec<TaskDefinition2>,
        remote_checkpoint_dir: String,
    ) -> PyResult<Self> {
        let remote_checkpoint_storage = Arc::new(FileSystemStateStorage::new(
            Arc::new(PrefixedLocalFileSystemStorage::new(remote_checkpoint_dir.clone())),
            "state", // Matches a fixed prefix in the worker process constructor
        ));
        let processors = compute_env.start_many_processors(tasks.len(), Some(remote_checkpoint_dir.clone())).await?;

        let assigned_tasks = tasks.iter()
            .zip(processors.iter().map(|processor| processor.addr().to_string()))
            .collect::<Vec<_>>();
        let scheduling_details = schedule_with_constant_partitions(&assigned_tasks, PartitionRange::full());

        // Start the tasks in the background
        for ((task, addr), processor) in assigned_tasks.iter().zip(processors.iter()) {
            println!("Assigning task {} to processor {}", task.task_id, addr);
            processor.update_plan(task, &scheduling_details).await?;
        }

        Ok(Self {
            compute_env,
            remote_checkpoint_storage,
            remote_checkpoint_dir,
            tasks: assigned_tasks.into_iter()
                .zip(processors.into_iter())
                .map(|((task, addr), processor)| (task.clone(), vec![(addr, processor, PartitionRange::full())]))
                .collect()
        })
    }

    pub async fn stream_last_output(&self) -> PyResult<impl Stream<Item=Result<RecordBatch, DataFusionError>> + use<>> {
        let (output_task, copies) = self.tasks.last()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("No tasks assigned"))?;
        if copies.len() != 1 {
            return Err(pyo3::exceptions::PyValueError::new_err("Expected exactly one copy of the output task"));
        }

        let (output_addr, _processor, _partition_range) = copies.first().unwrap();
        let outputs = output_task.exchange_outputs();
        let last_output = outputs.last().unwrap();
        let stream = stream_results(&output_addr, last_output)
            .await
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("Error streaming results: {}", e)))?;
        Ok(stream)
    }

    pub async fn scale_up(&mut self, task_id: &str, new_number: usize) -> PyResult<()> {
        // Find the task to scale up
        let (task_def, copies) = self.tasks.iter()
            .find(|(task, _)| task.task_id == task_id)
            .ok_or(pyo3::exceptions::PyValueError::new_err(format!("Task with ID {} not found", task_id)))?;

        // Create n copies of the task with the new partitioning
        // When scaling we always use 2^32 partitions as maybe there is no reason to use any other
        // number
        let new_partition_cap = 2usize^32;
        let partition_size_floor = new_partition_cap / new_number;
        let remaining_size = new_partition_cap % new_number;
        let partitions = (0..new_number).map(|i| {
            let size = partition_size_floor + if i < remaining_size { 1 } else { 0 };
            let start = i * partition_size_floor + if i < remaining_size { i } else { remaining_size };
            PartitionRange::new(start, start + size, new_partition_cap)
        }).collect::<Vec<_>>();

        // Find the most recently completed checkpoint for each partition
        let state_ids = task_def.used_state_ids();
        // TODO support multiple stats here
        if state_ids.len() != 1 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "Scaling up is only supported for tasks with a single state ID",
            ));
        }
        let task_state_id = &state_ids[0];
        let partitions = partitions.into_iter()
            .map(|partition| {
                let remote_checkpoint_storage = self.remote_checkpoint_storage.clone();
                async move {
                    let (checkpoint_id, _) = remote_checkpoint_storage.get_latest_operator_checkpoints(
                        task_state_id,
                        &partition,
                    ).await?;
                    // TODO the checkpoint_id should eventually be a string everywhere
                    // Parse checkpoint id to usize
                    let checkpoint_id = checkpoint_id.parse::<usize>()
                        .map_err(|e| internal_datafusion_err!("Failed to parse checkpoint ID: {}", e))?;
                    Ok::<(PartitionRange, usize), DataFusionError>((partition, checkpoint_id))
                }
            })
            .collect::<FuturesOrdered<_>>()
            .try_collect::<Vec<_>>()
            .await?;

        let processors = self.compute_env.start_many_processors(new_number, Some(self.remote_checkpoint_dir.clone())).await?;
        let assigned_tasks = partitions.into_iter()
            .zip(processors.into_iter())
            .map(|((partition, checkpoint), processor)| {
                (processor.addr().to_string(), processor, partition, checkpoint)
            })
            .collect::<Vec<_>>();

        // Start each of the tasks
        let input_locations = self.tasks.iter()
            .filter(|(task, _)| task.task_id != task_id)
            .flat_map(|(task, copies)| {
                // For each task output, create a generation input detail
                task.exchange_outputs()
                    .iter()
                    .map(|stream_id| {
                        GenerationInputDetail {
                            stream_id: stream_id.clone(),
                            locations: copies.iter().map(|(addr, _, partition_range)| GenerationInputLocation {
                                address: addr.clone(),
                                offset_range: (0, 2 << 31),
                                partitions: partition_range.clone(),
                            }).collect(),
                        }
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        for (addr, processor, partition, checkpoint) in assigned_tasks.iter() {
            println!("Scaling up task {} to processor {} with partition {:?}", task_def.task_id, addr, partition);
            let generation = GenerationSpec {
                id: format!("{}-{}", task_def.task_id, addr),
                partitions: partition.clone(),
                start_conditions: vec![],
            };
            processor.update_plan_with_checkpoint(
                task_def,
                &InitialSchedulingDetails {
                    generations: vec![generation],
                    input_locations: input_locations.clone(),
                },
                *checkpoint,
            ).await?;
        }

        Ok(())
    }
}

struct CoordinatorLogger {
    active_coordinator: Arc<ActiveCoordinator>,
    background_handle: Option<JoinHandle<()>>,
}

impl CoordinatorLogger {
    pub fn start(active_coordinator: Arc<ActiveCoordinator>) -> Self {
        let background_handle = tokio::spawn({
            let active_coordinator = active_coordinator.clone();
            async move {
                Self::print_loop(active_coordinator).await;
            }
        });
        Self {
            active_coordinator,
            background_handle: Some(background_handle),
        }
    }

    async fn print_loop(active_coordinator: Arc<ActiveCoordinator>) {
        // Loop forever until the tokio task is cancelled
        loop {
            // Log the tasks and their copies
            Self::log_tasks(active_coordinator.clone());

            // Sleep for a while before logging again
            sleep(std::time::Duration::from_secs(3)).await;
        }
    }

    pub fn log_tasks(active_coordinator: Arc<ActiveCoordinator>) {
        for (task, copies) in &active_coordinator.tasks {
            println!("Task ID: {}", task.task_id);
            for (addr, _, partition_range) in copies {
                println!("  Processor: {}, Partition Range: {:?}", addr, partition_range);
            }
        }
    }
}

impl Drop for CoordinatorLogger {
    fn drop(&mut self) {
        if let Some(handle) = self.background_handle.take() {
            handle.abort();
        }
    }
}
