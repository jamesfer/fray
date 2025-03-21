use std::sync::Arc;
use datafusion::execution::SendableRecordBatchStream;
use crate::action_stream::SendableActionStream;
use crate::worker_interface::WorkerInterface;

pub struct InputManager {
    worker_interface: Arc<WorkerInterface>,
}

impl InputManager {
    pub fn new(
        worker_interface: Arc<WorkerInterface>,
    ) -> Self {
        Self {
            worker_interface,
        }
    }

    pub async fn stream_input(&self, address: &String, input_stream_id: &String, partition: usize) -> Result<SendableActionStream, ()> {
        self.worker_interface.stream_output(address, input_stream_id, partition).await
    }
}
