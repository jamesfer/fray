use std::sync::Arc;
use pyo3::{PyObject, Python, Py, PyAny, Bound, PyResult};
use crate::python::py_utils::schedule_without_partitions_inner;
use crate::python::remote_processor::RemoteProcessor;
use crate::streaming::partitioning::PartitionRange;
use crate::streaming::task_definition_2::TaskDefinition2;
use crate::streaming::worker_process::InitialSchedulingDetails;

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
        let processor = RemoteProcessor::start(self.python_resources.clone()).await?;
        processor.update_plan(&task_definition, &initial_scheduling_details).await?;
        Ok(processor)
    }

    pub async fn start_many_processors(
        &self,
        num_processors: usize,
    ) -> PyResult<Vec<RemoteProcessor>> {
        RemoteProcessor::start_many(self.python_resources.clone(), num_processors).await
    }
}

pub struct StaticCoordinator {
    compute_env: Arc<ComputeEnv>,
}

impl StaticCoordinator {
    pub fn new(compute_env: Arc<ComputeEnv>) -> Self {
        Self { compute_env }
    }

    pub async fn schedule_together(&self, tasks: &[TaskDefinition2]) -> PyResult<Vec<(&TaskDefinition2, String, RemoteProcessor)>> {
        let processors = self.compute_env.start_many_processors(tasks.len()).await?;

        let assigned_tasks = tasks.into_iter()
            .zip(processors.iter().map(|processor| processor.addr().to_string()))
            .collect::<Vec<_>>();
        let scheduling_details = schedule_without_partitions_inner(&assigned_tasks);

        // Start the tasks in the background
        for ((task, addr), processor) in assigned_tasks.iter().zip(processors.iter()) {
            println!("Assigning task {} to processor {}", task.task_id, addr);
            processor.update_plan(task, &scheduling_details).await?;
        }

        Ok(assigned_tasks.into_iter()
            .zip(processors.into_iter())
            .map(|((task, addr), processor)| (task, addr, processor))
            .collect())
    }
}

pub struct ActiveCoordinator {
    compute_env: Arc<ComputeEnv>,
    tasks: Vec<(TaskDefinition2, Vec<(String, RemoteProcessor, PartitionRange)>)>,
}

impl ActiveCoordinator {
    pub async fn start_single(
        compute_env: Arc<ComputeEnv>,
        tasks: Vec<TaskDefinition2>,
    ) -> PyResult<Self> {
        let processors = compute_env.start_many_processors(tasks.len()).await?;

        let assigned_tasks = tasks.iter()
            .zip(processors.iter().map(|processor| processor.addr().to_string()))
            .collect::<Vec<_>>();
        let scheduling_details = schedule_without_partitions_inner(&assigned_tasks);

        // Start the tasks in the background
        for ((task, addr), processor) in assigned_tasks.iter().zip(processors.iter()) {
            println!("Assigning task {} to processor {}", task.task_id, addr);
            processor.update_plan(task, &scheduling_details).await?;
        }

        Ok(Self {
            compute_env,
            tasks: assigned_tasks.into_iter()
                .zip(processors.into_iter())
                .map(|((task, addr), processor)| (task, addr, processor))
                .collect()
        })
    }

    pub async fn scale_up(&self, task_id: &str, new_number: usize) -> PyResult<()> {
        // Find the task to scale up
        let task = self.tasks.iter()
            .find(|(task, _)| task.task_id == task_id)
            .ok_or(pyo3::exceptions::PyValueError::new_err(format!("Task with ID {} not found", task_id)))?;

        // Create n copies of the task with the new partitioning
        let existing_partition_cap = task.1.first()
            .ok_or(pyo3::exceptions::PyValueError::new_err("Cannot scale up a task with no existing instances".to_string()))?
            .2
            .partitions();
        if existing_partition_cap % new_number != 0 || new_number > existing_partition_cap {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "Cannot scale up task {} to {} partitions, existing partition cap is {}",
                task_id, new_number, existing_partition_cap
            )));
        }
        let new_partitions_per_task = existing_partition_cap / new_number;

        // Find the most recent available checkpoint for each partition

        // Find all inputs and outputs of the task


    }
}
