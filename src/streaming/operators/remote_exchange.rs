use crate::streaming::action_stream::StreamItem;
use crate::streaming::generation::{GenerationInputDetail, GenerationSpec};
use crate::streaming::operators::task_function::{CreateOperatorFunction2, OperatorFunction2, SItem};
use crate::streaming::operators::utils::fiber_stream::FiberStream;
use crate::streaming::runtime::{DataChannelSender, Runtime};
use async_trait::async_trait;
use datafusion::common::DataFusionError;
use eyeball::{AsyncLock, SharedObservable, Subscriber};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone, Serialize, Deserialize)]
pub struct RemoteExchangeOperator {
    stream_id: String,
}

impl RemoteExchangeOperator {
    pub fn new(stream_id: String) -> Self {
        Self { stream_id }
    }

    pub fn get_stream_id(&self) -> &str {
        &self.stream_id
    }
}

impl CreateOperatorFunction2 for RemoteExchangeOperator {
    fn create_operator_function(&self) -> Box<dyn OperatorFunction2 + Sync + Send> {
        Box::new(RemoteExchangeOperatorFunction::new(self.stream_id.clone()))
    }
}

struct RemoteExchangeOperatorFunction {
    stream_id: String,
    runtime: Option<Arc<Runtime>>,
    scheduling_details_state: Option<Arc<Mutex<(Vec<GenerationSpec>, Vec<GenerationInputDetail>)>>>,
    loaded_checkpoint: usize,
    data_channel: Option<DataChannelSender>,
    scheduling_details: Option<Subscriber<(Option<Vec<GenerationSpec>>, Option<Vec<GenerationInputDetail>>), AsyncLock>>,
}

impl RemoteExchangeOperatorFunction {
    fn new(stream_id: String) -> Self {
        Self {
            stream_id,
            runtime: None,
            scheduling_details_state: None,
            loaded_checkpoint: 0,
            data_channel: None,
            scheduling_details: None,
        }
    }
}

#[async_trait]
impl OperatorFunction2 for RemoteExchangeOperatorFunction {
    async fn init(
        &mut self,
        runtime: Arc<Runtime>,
        scheduling_details: SharedObservable<(Option<Vec<GenerationSpec>>, Option<Vec<GenerationInputDetail>>), AsyncLock>,
    ) -> Result<(), DataFusionError> {
        let scheduling_details_subscriber = scheduling_details.subscribe().await;
        let details = scheduling_details_subscriber.get().await;
        let generations = details.0.as_ref().unwrap();
        let generation = &generations[0];
        let partitions = generation.partitions.clone();
        let channel = runtime.data_exchange_manager().create_channel(self.stream_id.clone(), partitions).await;
        self.runtime = Some(runtime);
        self.data_channel = Some(channel);
        self.scheduling_details = Some(scheduling_details_subscriber);
        Ok(())
    }

    async fn load(&mut self, checkpoint: usize) -> Result<(), DataFusionError> {
        // No-op for now
        Ok(())
    }

    async fn run<'a>(
        &'a mut self,
        mut inputs: Vec<(usize, Box<dyn FiberStream<Item=Result<SItem, DataFusionError>> + Send + Sync + 'a>)>,
    ) -> Result<Vec<(usize, Box<dyn FiberStream<Item=Result<SItem, DataFusionError>> + Send + Sync + 'a>)>, DataFusionError> {
        let channel = self.data_channel.as_mut().ok_or_else(|| {
            DataFusionError::Execution("Data channel not initialized".to_string())
        })?;

        assert_eq!(inputs.len(), 1, "RemoteExchangeOperatorFunction should only have one input");
        let mut input_stream = inputs.pop().unwrap().1;
        let mut combined_stream = Box::into_pin(input_stream.as_mut().combined()?);
        while let Some(result) = combined_stream.next().await {
            match result {
                Ok(SItem::Generation(_)) => {
                    panic!("RemoteExchangeOperatorFunction does not know how to handle generations right now");
                },
                Ok(SItem::RecordBatch(record_batch)) => {
                    println!("Writing record to exchange channel: {}", record_batch.num_rows());
                    channel.send(Ok(StreamItem::RecordBatch(record_batch))).await;
                },
                Ok(SItem::Marker(marker)) => {
                    channel.send(Ok(StreamItem::Marker(marker))).await;
                },
                Err(err) => {
                    channel.send(Err(err)).await;
                },
            }
        }

        Ok(vec![])
    }

    async fn last_checkpoint(&self) -> usize {
        0
    }

    async fn close(self: Box<Self>) {
        // No-op
    }
}
