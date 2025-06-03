use std::sync::Arc;
use arrow_flight::Ticket;
use async_trait::async_trait;
use log::trace;
use prost::Message;
use tonic::{Request, Response, Status};
use crate::flight::{DoGetStream, FlightHandler};
use crate::proto::generated::streaming::StreamingFlightTicketData;
use crate::streaming::output_manager::OutputManager;
use crate::streaming::processor::stream_serialization::encode_stream_to_flight;

pub struct ProcessorFlightHandler {
    processor_name: String,
    output_manager: Arc<OutputManager>,
}

impl ProcessorFlightHandler {
    pub fn new(processor_name: String, output_manager: Arc<OutputManager>) -> Self {
        Self {
            processor_name,
            output_manager,
        }
    }
}

fn extract_ticket(ticket: Ticket) -> anyhow::Result<StreamingFlightTicketData> {
    let data = ticket.ticket;
    let tic = StreamingFlightTicketData::decode(data)?;
    Ok(tic)
}

#[async_trait]
impl FlightHandler for ProcessorFlightHandler {
    async fn get_stream(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<DoGetStream>, Status> {
        let remote_addr = request
            .remote_addr()
            .map(|a| a.to_string())
            .unwrap_or("unknown".to_string());

        let ticket = request.into_inner();
        let ticket = extract_ticket(ticket).map_err(|e| {
            Status::internal(format!(
                "{}, Unexpected error extracting ticket {e}",
                self.processor_name
            ))
        })?;

        let action_stream = self
            .output_manager
            .stream_output(&ticket.stream_id, ticket.partitions[0] as usize, None)
            .await
            .map_err(|e| {
                Status::internal(format!(
                    "{}, Error getting stream output",
                    self.processor_name
                ))
            })?;

        trace!(
            "{}, request for stream {} partition {:?} from {}",
            self.processor_name, ticket.stream_id, ticket.partitions, remote_addr
        );

        let encoded_stream = encode_stream_to_flight(action_stream);
        Ok(Response::new(Box::pin(encoded_stream)))
    }
}
