use async_trait::async_trait;
use arrow::array::RecordBatch;
use std::sync::Arc;
use arrow::datatypes::Schema;
use datafusion::common::DataFusionError;
use crate::streaming::action_stream::StreamItem;
use crate::streaming::tasks::serialization::ProtoSerializer;
use crate::streaming::tasks::task_function::{OutputChannel, OutputChannelL, TaskFunction, TaskState};
use crate::proto::generated::streaming_tasks as proto;

#[derive(Clone)]
pub struct IdentityOperator;

impl IdentityOperator {
    pub fn into_function(self) -> IdentityTask {
        IdentityTask
    }
}

impl ProtoSerializer for IdentityOperator {
    type ProtoType = proto::IdentityOperator;
    type SerializerContext<'a> = ();
    type DeserializerContext<'a> = ();

    fn try_into_proto(self, _context: &Self::SerializerContext<'_>) -> Result<Self::ProtoType, DataFusionError> {
        Ok(proto::IdentityOperator {})
    }

    fn try_from_proto(_proto: Self::ProtoType, _context: &Self::DeserializerContext<'_>) -> Result<Self, DataFusionError> {
        Ok(IdentityOperator)
    }
}

pub struct IdentityTask;

#[async_trait]
impl TaskFunction for IdentityTask {
    async fn init(&mut self) {}

    async fn poll(&mut self, output: &mut OutputChannelL) -> TaskState {
        unimplemented!()
    }

    async fn process(&mut self, data: RecordBatch, ordinal: usize, output: &mut OutputChannel) -> TaskState {
        output(StreamItem::RecordBatch(data)).await;
        TaskState::Continue
    }

    async fn get_state(&mut self) -> RecordBatch {
        RecordBatch::new_empty(Arc::new(Schema::empty()))
    }

    async fn load_state(&mut self, state: RecordBatch) {}

    async fn finish(&mut self, output: &mut OutputChannel) {}
}
