use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use arrow_flight::{FlightClient, Ticket};
use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use prost::Message;
use tokio::sync::Mutex;
use datafusion::common::internal_datafusion_err;
use datafusion::error::DataFusionError;
use crate::proto::generated::streaming::StreamingFlightTicketData;
use crate::streaming::operators::task_function::{CreateOperatorFunction2, OperatorFunction2, SItem};
use crate::streaming::runtime::Runtime;
use crate::util::make_client;

pub struct RemoteSourceOperator {
    stream_ids: Vec<String>,
}

impl CreateOperatorFunction2 for RemoteSourceOperator {
    fn create_operator_function(&self) -> Box<dyn OperatorFunction2 + Sync + Send> {
        Box::new(RemoteSourceOperatorFunction::new(self.stream_ids.clone()))
    }
}

struct RemoteSourceOperatorFunction {
    stream_ids: Vec<String>,
    runtime: Option<Arc<Runtime>>,
}

impl RemoteSourceOperatorFunction {
    fn new(stream_ids: Vec<String>) -> Self {
        Self { stream_ids, runtime: None }
    }

    async fn stream_batches_from(&self, address: &str, stream_id: String, partition: usize) -> Result<impl Stream<Item=SItem>, DataFusionError> {
        let inner_ticket = StreamingFlightTicketData {
            partition: partition as u64,
            stream_id: stream_id.to_string(),
        };
        let ticket = Ticket { ticket: Bytes::from(inner_ticket.encode_to_vec()) };
        let data_client_manager = self.runtime.as_ref().unwrap().data_client_manager();
        let client = data_client_manager.get_client(address).await?;
        let client = &mut *client.lock().await;
        let flight_data_stream = client.do_get(ticket).await
            .map_err(|e| internal_datafusion_err!("Error getting flight stream: {}", e))?;
        Ok(Self::convert_to_sendable_action_stream(schema, flight_data_stream))
    }
}

#[async_trait]
impl OperatorFunction2 for RemoteSourceOperatorFunction {
    async fn init(&mut self, runtime: Arc<Runtime>) {
        self.runtime = Some(runtime);
    }

    async fn run<'a>(&'a mut self, inputs: Vec<(usize, Vec<Pin<Box<dyn Stream<Item=SItem> + Send + Sync + 'a>>>)>) -> Vec<(usize, Vec<Pin<Box<dyn Stream<Item=SItem> + Send + Sync + 'a>>>)> {
        assert_eq!(inputs.len(), 0);

        // self.stream_batches_from(self.stream_ids)

        // TODO
        let address = self.stream_id_map.get(input_stream_id).ok_or(internal_datafusion_err!("Stream ID not found"))?;
    }

    async fn close(self: Box<Self>) {
        // No-op
    }
}
