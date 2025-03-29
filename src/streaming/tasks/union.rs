use std::sync::Arc;
use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use async_trait::async_trait;
use datafusion::common::DataFusionError;
use crate::streaming::action_stream::StreamItem;
use crate::streaming::tasks::serialization::ProtoSerializer;
use crate::streaming::tasks::task_function::{OutputChannel, OutputChannelL, TaskFunction, TaskState};
use crate::proto::generated::streaming_tasks as proto;

#[derive(Clone)]
pub struct UnionOperator;

impl UnionOperator {
    pub fn into_function(self) -> UnionTask {
        UnionTask
    }
}

impl ProtoSerializer for UnionOperator {
    type ProtoType = proto::UnionOperator;
    type SerializerContext<'a> = ();
    type DeserializerContext<'a> = ();

    fn try_into_proto(self, _context: &Self::SerializerContext<'_>) -> Result<Self::ProtoType, DataFusionError> {
        Ok(proto::UnionOperator {})
    }

    fn try_from_proto(_proto: Self::ProtoType, _context: &Self::DeserializerContext<'_>) -> Result<Self, DataFusionError> {
        Ok(UnionOperator)
    }
}

pub struct UnionTask;

#[async_trait]
impl TaskFunction for UnionTask {
    async fn init(&mut self) {}

    async fn poll(&mut self, output: &mut OutputChannelL) -> TaskState {
        unimplemented!()
    }

    async fn process(&mut self, data: RecordBatch, _input_channel: usize, output: &mut OutputChannel) -> TaskState {
        output(StreamItem::RecordBatch(data)).await;
        TaskState::Continue
    }

    async fn finish(&mut self, output: &mut OutputChannel) {}

    async fn get_state(&mut self) -> RecordBatch {
        RecordBatch::new_empty(Arc::new(Schema::empty()))
    }

    async fn load_state(&mut self, state: RecordBatch) {}
}
