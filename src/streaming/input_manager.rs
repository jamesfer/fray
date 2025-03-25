use std::collections::HashMap;
use std::sync::Arc;
use arrow::datatypes::SchemaRef;
use arrow_flight::{FlightClient, Ticket};
use arrow_flight::decode::{DecodedFlightData, DecodedPayload, FlightRecordBatchStream};
use bytes::Bytes;
use datafusion::common::{internal_datafusion_err, DataFusionError};
use futures::TryStreamExt;
use itertools::Itertools;
use prost::Message;
use tokio::sync::Mutex;
use crate::proto::generated::streaming::StreamingFlightTicketData;
use crate::streaming::action_stream::{ActionStreamAdapter, Marker, SendableActionStream, StreamItem};
use crate::streaming::processor::stream_serialization::decode_flight_to_stream;
use crate::util::make_client;

pub struct InputManager {
    client_map: HashMap<String, Mutex<FlightClient>>,
    stream_id_map: HashMap<String, String>,
}

impl InputManager {
    pub async fn new(
        output_stream_address_map: HashMap<String, String>,
    ) -> Result<Self, DataFusionError> {
        let clients = output_stream_address_map.values().cloned().dedup().collect();
        Ok(Self {
            client_map: Self::connect_to(clients).await?,
            stream_id_map: output_stream_address_map,
        })
    }

    pub async fn stream_input(&self, _address: &String, input_stream_id: &String, partition: usize, schema: SchemaRef) -> Result<SendableActionStream, DataFusionError> {
        let address = self.stream_id_map.get(input_stream_id).ok_or(internal_datafusion_err!("Stream ID not found"))?;
        let client = self.client_map.get(address).ok_or(internal_datafusion_err!("Client not found"))?;
        let inner_ticket = StreamingFlightTicketData {
            partition: partition as u64,
            stream_id: input_stream_id.clone(),
        };
        let ticket = Ticket { ticket: Bytes::from(inner_ticket.encode_to_vec()) };
        let client = &mut *client.lock().await;
        let flight_data_stream = client.do_get(ticket).await
            .map_err(|e| internal_datafusion_err!("Error getting flight stream: {}", e))?;
        Ok(Self::convert_to_sendable_action_stream(schema, flight_data_stream))
    }

    async fn connect_to(clients: Vec<String>) -> Result<HashMap<String, Mutex<FlightClient>>, DataFusionError> {
        let mut client_map = HashMap::new();
        for address in clients {
            let client = Mutex::new(make_client(address.as_str()).await?);
            client_map.insert(address, client);
        }
        Ok(client_map)
    }

    fn convert_to_sendable_action_stream(schema: SchemaRef, stream: FlightRecordBatchStream) -> SendableActionStream {
        let inner = decode_flight_to_stream(stream);
        Box::pin(ActionStreamAdapter::new(schema, inner))
    }
}
