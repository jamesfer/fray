use std::sync::Arc;
use futures::Stream;
use arrow_flight::Ticket;
use bytes::Bytes;
use datafusion::common::{internal_datafusion_err, DataFusionError};
use prost::Message;
use futures_util::StreamExt;
use tokio::sync::Mutex;
use crate::proto::generated::streaming::StreamingFlightTicketData;
use crate::streaming::action_stream::StreamItem;
use crate::streaming::operators::task_function::SItem;
use crate::streaming::utils::sync_safe::SyncSafe;
use crate::streaming::processor::stream_serialization::decode_flight_to_stream;
use crate::streaming::runtime::Runtime;
use crate::util::make_client;

pub async fn create_remote_stream_no_runtime(
    stream_id: &str,
    address: &str,
    partitions: Vec<usize>,
) -> Result<Box<dyn Stream<Item=Result<SItem, DataFusionError>> + Send + Sync>, DataFusionError> {
    let ticket_data = StreamingFlightTicketData {
        partitions: partitions.into_iter().map(|p| p as u64).collect(),
        stream_id: stream_id.to_string(),
    };
    let flight_ticket = Ticket { ticket: Bytes::from(ticket_data.encode_to_vec()) };
    let client = Arc::new(Mutex::new(make_client(address).await?));
    let client = &mut *client.lock().await;
    let flight_data_stream = client.do_get(flight_ticket).await
        .map_err(|e| internal_datafusion_err!("Error getting flight stream: {}", e))?;
    let record_batch_stream = SyncSafe::new(decode_flight_to_stream(flight_data_stream));
    Ok(Box::new(record_batch_stream.map(|result| {
        match result {
            Ok(StreamItem::RecordBatch(batch)) => Ok(SItem::RecordBatch(batch)),
            Ok(StreamItem::Marker(marker)) => Ok(SItem::Marker(marker)),
            Err(err) => Err(err),
        }
    })))
}

// TODO add checkpoint
pub async fn create_remote_stream(
    runtime: &Runtime,
    stream_id: &str,
    address: &str,
    partitions: Vec<usize>,
) -> Result<Box<dyn Stream<Item=Result<SItem, DataFusionError>> + Send + Sync>, DataFusionError> {
    let ticket_data = StreamingFlightTicketData {
        partitions: partitions.into_iter().map(|p| p as u64).collect(),
        stream_id: stream_id.to_string(),
    };
    let flight_ticket = Ticket { ticket: Bytes::from(ticket_data.encode_to_vec()) };
    let client = runtime.data_client_manager().get_client(address).await?;
    let client = &mut *client.lock().await;
    let flight_data_stream = client.do_get(flight_ticket).await
        .map_err(|e| internal_datafusion_err!("Error getting flight stream: {}", e))?;
    let record_batch_stream = SyncSafe::new(decode_flight_to_stream(flight_data_stream));
    Ok(Box::new(record_batch_stream.map(|result| {
        match result {
            Ok(StreamItem::RecordBatch(batch)) => Ok(SItem::RecordBatch(batch)),
            Ok(StreamItem::Marker(marker)) => Ok(SItem::Marker(marker)),
            Err(err) => Err(err),
        }
    })))
}
