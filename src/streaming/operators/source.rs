use crate::streaming::generation::{GenerationInputDetail, GenerationSpec};
use crate::streaming::operators::task_function::{CreateOperatorFunction2, OperatorFunction2, SItem};
use crate::streaming::operators::utils::fiber_stream::{FiberStream, SingleFiberStream};
use crate::streaming::runtime::Runtime;
use arrow::array::{Array, RecordBatch};
use async_trait::async_trait;
use datafusion::common::DataFusionError;
use eyeball::{AsyncLock, SharedObservable};
use futures::stream::iter;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use crate::streaming::action_stream::StreamItem;

#[derive(Clone, Serialize, Deserialize)]
pub struct SourceOperator {
    items: Vec<StreamItem>,
}

impl SourceOperator {
    pub fn new(data: Vec<RecordBatch>) -> Self {
        Self::new_with_markers(data.into_iter().map(|batch| StreamItem::RecordBatch(batch)).collect())
    }

    pub fn new_with_markers(items: Vec<StreamItem>) -> Self {
        Self {
            items,
        }
    }

    pub fn into_function(self) -> SourceOperatorFunction {
        SourceOperatorFunction::new(self.items)
    }
}

impl CreateOperatorFunction2 for SourceOperator {
    fn create_operator_function(&self) -> Box<dyn OperatorFunction2 + Sync + Send> {
        Box::new(SourceOperatorFunction::new(self.items.clone()))
    }
}

struct SourceOperatorFunction {
    items: Vec<StreamItem>,
    offset: usize,
}

impl SourceOperatorFunction {
    pub fn new(items: Vec<StreamItem>) -> Self {
        Self {
            items,
            offset: 0,
        }
    }
}

#[async_trait]
impl OperatorFunction2 for SourceOperatorFunction {
    async fn init(
        &mut self,
        _runtime: Arc<Runtime>,
        _scheduling_details: SharedObservable<(Option<Vec<GenerationSpec>>, Option<Vec<GenerationInputDetail>>), AsyncLock>
    ) -> Result<(), DataFusionError> {
        Ok(())
    }

    async fn load(&mut self, checkpoint: usize) -> Result<(), DataFusionError> {
        // Move the offset to the matching marker in the static list
        if checkpoint > 0 {
            let (index, _) = self.items.iter().enumerate()
                .find(|(_, item)| match item {
                    StreamItem::Marker(marker) => marker.checkpoint_number as usize == checkpoint,
                    _ => false,
                })
                .ok_or_else(|| DataFusionError::Execution(format!("No marker found for checkpoint {}", checkpoint)))?;
            self.offset = index;
            println!("SourceOperatorFunction: Loaded checkpoint {} at offset {}", checkpoint, self.offset);
        } else {
            self.offset = 0;
        }

        Ok(())
    }

    async fn run<'a>(
        &'a mut self,
        inputs: Vec<(usize, Box<dyn FiberStream<Item=Result<SItem, DataFusionError>> + Send + Sync + 'a>)>,
    ) -> Result<Vec<(usize, Box<dyn FiberStream<Item=Result<SItem, DataFusionError>> + Send + Sync + 'a>)>, DataFusionError> {
        assert_eq!(inputs.len(), 0, "Source operator should not have any inputs");

        println!("SourceOperatorFunction: Running from offset {}", self.offset);
        let record_batch_iter = self.items[self.offset..].iter()
            .map(|batch| {
                match batch {
                    StreamItem::RecordBatch(batch) => Ok(SItem::RecordBatch(batch.clone())),
                    StreamItem::Marker(marker) => Ok(SItem::Marker(marker.clone())),
                }
            });
        Ok(vec![(0, Box::new(SingleFiberStream::new(iter(record_batch_iter))))])
    }

    async fn last_checkpoint(&self) -> usize {
        0
    }

    async fn close(self: Box<Self>) {
        // No-op
    }
}
