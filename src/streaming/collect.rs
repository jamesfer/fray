use arrow::datatypes::SchemaRef;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::common::DataFusionError;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures_util::TryStreamExt;
use crate::streaming::action_stream::StreamItem;
use crate::streaming::input_manager::InputManager;

pub async fn collect_from_stage_streaming(
    stage_id: usize,
    addr: &str,
    output_stream_id: &str,
    schema: SchemaRef,
) -> Result<SendableRecordBatchStream, DataFusionError> {
    let map = [(output_stream_id.to_string(), addr.to_string())]
        .iter()
        .cloned()
        .collect();
    let input_manager = InputManager::new(map).await?;
    let stream = input_manager.stream_input(&"".to_string(), &output_stream_id.to_string(), 0, schema.clone()).await?;
    let stream = stream.try_filter_map(|x| async {
        match x {
            StreamItem::Marker(_) => Ok(None),
            StreamItem::RecordBatch(record_batch) => Ok(Some(record_batch)),
        }
    });

    Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
}
