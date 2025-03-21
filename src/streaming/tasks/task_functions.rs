use std::sync::Arc;
use async_trait::async_trait;
use datafusion::arrow;
use datafusion::arrow::array::{ArrayRef, AsArray, RecordBatch, UInt64Array};
use datafusion::arrow::datatypes::Schema;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExprRef;
use crate::action_stream::{Marker, StreamItem};
use crate::task_function::{OutputChannel, OutputChannelL, TaskFunction, TaskState};

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


pub struct SourceTask {
    data: Vec<RecordBatch>,
    offset: usize,
}

impl SourceTask {
    pub fn new(data: Vec<RecordBatch>) -> Self {
        Self {
            data,
            offset: 0,
        }
    }
}

#[async_trait]
impl TaskFunction for SourceTask {
    async fn init(&mut self) {}

    async fn poll(&mut self, output: &mut OutputChannelL) -> TaskState {
        match self.data.get(self.offset) {
            Some(next_batch) => {
                output(StreamItem::RecordBatch(self.data[self.offset].clone())).await;
                output(StreamItem::Marker(Marker { checkpoint_number: self.offset as u64 + 1 })).await;
                self.offset += 1;
                TaskState::Continue
            }
            None => TaskState::Exhausted,
        }
    }

    async fn process(&mut self, data: RecordBatch, ordinal: usize, output: &mut OutputChannel) -> TaskState {
        // This should never be called as this task should never have any inputs
        unimplemented!()
    }

    async fn finish(&mut self, output: &mut OutputChannel) {}

    async fn get_state(&mut self) -> RecordBatch {
        println!("Building state with offset of {}", self.offset);
        RecordBatch::try_from_iter([
            ("hash", Arc::new(UInt64Array::from(vec![0])) as ArrayRef),
            ("offset", Arc::new(UInt64Array::from(vec![self.offset as u64])) as ArrayRef),
        ]).unwrap()
    }

    async fn load_state(&mut self, state: RecordBatch) {
        let column = state.column(1).as_any().downcast_ref::<UInt64Array>().unwrap();
        let value = column.value(0);
        println!("Loading state with offset of {}", value);
        self.offset = value as usize;
    }
}

pub struct FilterTask {
    expression: PhysicalExprRef,
}

impl FilterTask {
    pub fn new(expression: PhysicalExprRef) -> Self {
        Self {
            expression,
        }
    }
}

#[async_trait]
impl TaskFunction for FilterTask {
    async fn init(&mut self) {}

    async fn poll(&mut self, output: &mut OutputChannelL) -> TaskState {
        unimplemented!()
    }

    async fn process(&mut self, data: RecordBatch, ordinal: usize, output: &mut OutputChannel) -> TaskState {
        let result = self.expression.evaluate(&data).unwrap();
        match result {
            ColumnarValue::Scalar(scalar) => {
                match scalar {
                    ScalarValue::Boolean(boolean) => {
                        if boolean.unwrap_or(false) {
                            output(StreamItem::RecordBatch(data)).await;
                        }
                    },
                    _ => { panic!("Expected boolean scalar value") },
                }
            },
            ColumnarValue::Array(array) => {
                let boolean_array = array.as_boolean();
                let result_batch = arrow::compute::filter_record_batch(&data, &boolean_array).unwrap();
                output(StreamItem::RecordBatch(result_batch)).await;
            },
        }

        TaskState::Continue
    }

    async fn finish(&mut self, output: &mut OutputChannel) {}

    async fn get_state(&mut self) -> RecordBatch {
        RecordBatch::new_empty(Arc::new(Schema::empty()))
    }

    async fn load_state(&mut self, state: RecordBatch) {
        unimplemented!()
    }
}
