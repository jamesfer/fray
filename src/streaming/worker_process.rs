use std::collections::HashMap;
use std::sync::Arc;
use datafusion::error::DataFusionError;
use datafusion::prelude::SessionContext;
use parking_lot::RwLock;
use crate::flight::FlightHandler;
use crate::streaming::checkpoint_storage_manager::CheckpointStorageManager;
use crate::streaming::input_manager::InputManager;
use crate::streaming::output_manager::OutputManager;
use crate::streaming::processor::flight_handler::ProcessorFlightHandler;
use crate::streaming::task_definition::TaskDefinition;
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

pub struct WorkerProcess {
    name: String,
    output_manager: Arc<OutputManager>,
    checkpoint_manager: Arc<CheckpointStorageManager>,
    runner: Arc<DFRayStreamingTaskRunner>,
}

impl WorkerProcess {
    pub fn new(name: String) -> Self {
        let output_manager = Arc::new(OutputManager::new());
        let checkpoint_manager = Arc::new(CheckpointStorageManager::new());
        let runner = Arc::new(DFRayStreamingTaskRunner::new(name.clone(), output_manager.clone(), checkpoint_manager.clone()));

        Self {
            name,
            output_manager,
            checkpoint_manager,
            runner,
        }
    }

    pub fn make_flight_handler(&self) -> Arc<dyn FlightHandler> {
        Arc::new(ProcessorFlightHandler::new(self.name.clone(), self.output_manager.clone()))
    }

    pub fn parse_task_definition(&self, definition_bytes: &[u8]) -> Result<TaskDefinition, DataFusionError> {
        TaskDefinition::try_decode_from_bytes(definition_bytes, &SessionContext::new())
    }

    pub async fn start_task(
        &self,
        stage_id: usize,
        output_stream_address_map: HashMap<String, String>,
        task_def: TaskDefinition,
    ) -> Result<(), DataFusionError> {
        self.runner.run_task(stage_id, output_stream_address_map, task_def).await
    }

    pub async fn update_input(&self) {}
}
