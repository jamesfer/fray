use std::pin::Pin;
use async_trait::async_trait;
use arrow::array::RecordBatch;
use std::sync::Arc;
use arrow::datatypes::Schema;
use eyeball::{AsyncLock, SharedObservable};
use flume::Receiver;
use futures::Stream;
use serde::{Deserialize, Serialize};
use datafusion::common::DataFusionError;
use crate::streaming::action_stream::StreamItem;
use crate::streaming::operators::serialization::ProtoSerializer;
use crate::streaming::operators::task_function::{CreateOperatorFunction2, OperatorFunction2, OutputChannel, OutputChannelL, SItem, TaskFunction, TaskState};
use crate::proto::generated::streaming_tasks as proto;
use crate::streaming::generation::{GenerationInputDetail, GenerationSpec, TaskSchedulingDetailsUpdate};
use crate::streaming::operators::utils::fiber_stream::FiberStream;
use crate::streaming::runtime::Runtime;

#[derive(Clone, Serialize, Deserialize)]
pub struct IdentityOperator;

impl IdentityOperator {
    pub fn into_function(self) -> IdentityTask {
        IdentityTask
    }
}

impl CreateOperatorFunction2 for IdentityOperator {
    fn create_operator_function(&self) -> Box<dyn OperatorFunction2 + Sync + Send> {
        Box::new(IdentityOperatorFunction)
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

struct IdentityOperatorFunction;

#[async_trait]
impl OperatorFunction2 for IdentityOperatorFunction {
    async fn init(
        &mut self,
        _runtime: Arc<Runtime>,
        _scheduling_details: SharedObservable<(Option<Vec<GenerationSpec>>, Option<Vec<GenerationInputDetail>>), AsyncLock>,
        _state_id: &str,
    ) -> Result<(), DataFusionError> {
        Ok(())
    }

    async fn load(&mut self, _checkpoint: usize) -> Result<(), DataFusionError> {
        Ok(())
    }

    async fn run<'a>(
        &'a mut self,
        inputs: Vec<(usize, Box<dyn FiberStream<Item=Result<SItem, DataFusionError>> + Send + Sync + 'a>)>,
    ) -> Result<Vec<(usize, Box<dyn FiberStream<Item=Result<SItem, DataFusionError>> + Send + Sync + 'a>)>, DataFusionError> {
        Ok(inputs)
    }

    async fn last_checkpoint(&self) -> usize {
        0
    }

    async fn close(self: Box<Self>) {
        // No-op
    }
}
