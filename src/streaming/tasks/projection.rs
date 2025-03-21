use std::sync::Arc;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::physical_expr::PhysicalExprRef;
use datafusion::physical_plan::streaming_operators::projection::ProjectionStreamingTask;
use crate::action_stream::StreamItem;
use crate::task_function::{OutputChannel, OutputChannelL, TaskFunction, TaskState};

pub struct ProjectionTaskSpec {
    schema: SchemaRef,
    expressions: Vec<PhysicalExprRef>
}

impl Into<Box<dyn TaskFunction + Sync + Send>> for ProjectionTaskSpec {
    fn into(self) -> Box<dyn TaskFunction + Sync + Send> {
        Box::new(ProjectionTask::new(self.schema, self.expressions))
    }
}

pub struct ProjectionTask {
    inner: ProjectionStreamingTask,
}

impl ProjectionTask {
    pub fn new(schema: SchemaRef, expressions: Vec<PhysicalExprRef>) -> Self {
        Self {
            inner: ProjectionStreamingTask::new(schema, expressions),
        }
    }
}

impl TaskFunction for ProjectionTask {
    async fn init(&mut self) {}

    async fn poll(&mut self, _output: &mut OutputChannelL) -> TaskState {
        unimplemented!()
    }

    async fn process(&mut self, data: RecordBatch, _input_channel: usize, output: &mut OutputChannel) -> TaskState {
        output(StreamItem::RecordBatch(self.inner.process_batch(&data).unwrap())).await;
        TaskState::Continue
    }

    async fn finish(&mut self, _output: &mut OutputChannel) {}

    async fn get_state(&mut self) -> RecordBatch {
        RecordBatch::new_empty(Arc::new(Schema::empty()))
    }

    async fn load_state(&mut self, _state: RecordBatch) {}
}
