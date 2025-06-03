use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;
use datafusion_python::utils::wait_for_future;
use local_ip_address::local_ip;
use datafusion::error::DataFusionError;
use datafusion::prelude::SessionContext;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use datafusion::common::internal_datafusion_err;
use crate::flight::FlightHandler;
use crate::streaming::checkpoint_storage_manager::CheckpointStorageManager;
use crate::streaming::generation::{GenerationInputDetail, GenerationSpec, TaskSchedulingDetailsUpdate};
use crate::streaming::input_manager::InputManager;
use crate::streaming::output_manager::OutputManager;
use crate::streaming::processor::flight_handler::ProcessorFlightHandler;
use crate::streaming::runtime::Runtime;
use crate::streaming::task_definition::TaskDefinition;
use crate::streaming::task_definition_2::TaskDefinition2;
use crate::streaming::task_main_3::RunningTask;
use crate::streaming::task_runner::TaskRunner;

struct DFRayStreamingTaskExecutor {
    stage_id: usize,
    output_stream_address_map: HashMap<String, String>,
    task_runner: TaskRunner,
}

impl DFRayStreamingTaskExecutor {
    pub fn start(
        stage_id: usize,
        output_stream_address_map: HashMap<String, String>,
        task_definition: TaskDefinition,
        input_manager: Arc<InputManager>,
        output_manager: Arc<OutputManager>,
        checkpoint_storage_manager: Arc<CheckpointStorageManager>,
    ) -> Result<Self, DataFusionError> {
        let function = task_definition.spec.into_task_function();
        let task_runner = TaskRunner::start(
            format!("stage_{}", stage_id),
            function,
            task_definition.inputs,
            task_definition.output_stream_id,
            task_definition.output_schema,
            task_definition.output_partitioning,
            task_definition.checkpoint_id,
            None,
            input_manager,
            output_manager,
            checkpoint_storage_manager,
        );

        Ok(Self {
            stage_id,
            output_stream_address_map,
            task_runner,
        })
    }
}

struct DFRayStreamingTaskRunner {
    /// our name, useful for logging
    name: String,
    /// Inner state of the handler
    inner: RwLock<Option<DFRayStreamingTaskExecutor>>,
    /// Output channels
    output_manager: Arc<OutputManager>,
    checkpoint_storage_manager: Arc<CheckpointStorageManager>,
}

impl DFRayStreamingTaskRunner {
    pub fn new(name: String, output_manager: Arc<OutputManager>, checkpoint_storage_manager: Arc<CheckpointStorageManager>) -> Self {
        let inner = RwLock::new(None);

        Self {
            name,
            inner,
            output_manager,
            checkpoint_storage_manager,
        }
    }

    async fn run_task(
        &self,
        stage_id: usize,
        output_stream_address_map: HashMap<String, String>,
        task_definition: TaskDefinition,
    ) -> Result<(), DataFusionError> {
        let input_manager = Arc::new(InputManager::new(output_stream_address_map.clone()).await?);

        // Wait to start the executor until we can acquire the lock
        let inner_ref = &mut *self.inner.write();
        let inner = DFRayStreamingTaskExecutor::start(
            stage_id,
            output_stream_address_map,
            task_definition,
            input_manager,
            self.output_manager.clone(),
            self.checkpoint_storage_manager.clone(),
        )?;
        inner_ref.replace(inner);

        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
pub struct InitialSchedulingDetails {
    pub generations: Vec<GenerationSpec>,
    pub input_locations: Vec<GenerationInputDetail>,
}

pub struct WorkerProcess {
    name: String,
    runtime: Arc<Runtime>,
    running_tasks: RwLock<HashMap<String, RunningTask>>,
}

impl WorkerProcess {
    pub async fn start(name: String) -> Result<Self, DataFusionError> {
        let name = format!("[{}]", name);
        let local_ip_addr = local_ip()
            .map_err(|err| internal_datafusion_err!("Failed to get worker local ip: {}", err))?;
        let runtime = Arc::new(Runtime::start(local_ip_addr).await?);

        Ok(Self {
            name,
            runtime,
            running_tasks: RwLock::new(HashMap::new()),
        })
    }

    pub fn data_exchange_address(&self) -> &str {
        self.runtime.data_exchange_manager().get_exchange_address()
    }

    #[cfg(test)]
    pub fn get_runtime(&self) -> Arc<Runtime> {
        self.runtime.clone()
    }

    pub async fn start_task(
        &self,
        task_definition: TaskDefinition2,
        initial_scheduling_details: InitialSchedulingDetails,
    ) -> Result<(), DataFusionError> {
        let running_task = RunningTask::start(
            task_definition.task_id.clone(),
            task_definition.operator,
            self.runtime.clone(),
            0, // initial checkpoint
            initial_scheduling_details.input_locations,
            initial_scheduling_details.generations,
        ).await?;

        let mut running_tasks = self.running_tasks.write();
        match running_tasks.entry(task_definition.task_id.clone()) {
            Entry::Occupied(_) => {
                running_task.cancel();
                Err(internal_datafusion_err!(
                    "Task with ID {} is already running",
                    task_definition.task_id
                ))
            }
            Entry::Vacant(entry) => {
                entry.insert(running_task);
                Ok(())
            },
        }
    }

    pub async fn update_input(&self, task_id: String, task_scheduling_details_update: TaskSchedulingDetailsUpdate) -> Result<(), DataFusionError> {
        let running_tasks = self.running_tasks.read();
        match running_tasks.get(&task_id) {
            None => {
                Err(internal_datafusion_err!(
                    "No running task found with ID {}",
                    task_id
                ))
            }
            Some(running_task) => {
                running_task.update_scheduling_details(task_scheduling_details_update).await;
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion::common::record_batch;
    use crate::streaming::generation::GenerationSpec;
    use crate::streaming::operators::nested::NestedOperator;
    use crate::streaming::operators::operator::{OperatorDefinition, OperatorInput, OperatorOutput, OperatorSpec};
    use crate::streaming::operators::remote_exchange::RemoteExchangeOperator;
    use crate::streaming::operators::source::SourceOperator;
    use crate::streaming::operators::task_function::SItem;
    use crate::streaming::task_definition_2::TaskDefinition2;
    use crate::streaming::utils::create_remote_stream::create_remote_stream;
    use crate::streaming::worker_process::{InitialSchedulingDetails, WorkerProcess};
    use futures::StreamExt;
    use std::future::Future;
    use std::time::Duration;
    use datafusion::error::DataFusionError;
    use tokio::time::sleep;

    #[tokio::test]
    pub async fn single_output_task() {
        let worker = WorkerProcess::start("worker1".to_string()).await.unwrap();

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

        worker.start_task(
            task_definition,
            InitialSchedulingDetails {
                generations: vec![GenerationSpec {
                    id: "gen1".to_string(),
                    partitions: vec![0],
                    start_conditions: vec![],
                }],
                input_locations: vec![],
            },
        ).await.unwrap();

        // Pull the output of the exchange operator
        let address = worker.data_exchange_address();
        let runtime = worker.get_runtime();

        let results_stream = retry_future(5, || {
            create_remote_stream(
                &runtime,
                "exchange_output1",
                address,
                vec![0],
            )
        }).await.unwrap();
        let results = Box::into_pin(results_stream).take(1).collect::<Vec<_>>().await;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].as_ref().unwrap(), &SItem::RecordBatch(test_batch));
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
}
