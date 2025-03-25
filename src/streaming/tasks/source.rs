use crate::proto::generated::streaming_tasks as proto;
use crate::streaming::action_stream::{Marker, StreamItem};
use crate::streaming::tasks::serialization::ProtoSerializer;
use crate::streaming::tasks::task_function::{OutputChannel, OutputChannelL, TaskFunction, TaskState};
use arrow::array::{Array, ArrayRef, RecordBatch, UInt64Array};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use async_trait::async_trait;
use datafusion::common::DataFusionError;
use std::sync::Arc;

#[derive(Clone)]
pub struct SourceOperator {
    data: Vec<RecordBatch>,
}

impl SourceOperator {
    pub fn new(data: Vec<RecordBatch>) -> Self {
        Self {
            data,
        }
    }

    pub fn into_function(self) -> SourceTask {
        SourceTask::new(self.data)
    }
}

impl ProtoSerializer for SourceOperator {
    type ProtoType = proto::SourceOperator;
    type SerializerContext<'a> = ();
    type DeserializerContext<'a> = ();

    fn try_into_proto(self, _context: &Self::SerializerContext<'_>) -> Result<Self::ProtoType, DataFusionError> {
        if self.data.is_empty() {
            return Ok(Self::ProtoType {
                record_batches: vec![],
            });
        }

        let schema = self.data[0].schema();
        let mut output = vec![];

        let mut writer = StreamWriter::try_new(&mut output, &schema)?;
        for batch in self.data {
            writer.write(&batch)?;
        }
        let bytes = writer.into_inner()?;
        Ok(Self::ProtoType {
            record_batches: vec![bytes.to_vec()],
        })
    }

    fn try_from_proto(proto: Self::ProtoType, _context: &Self::DeserializerContext<'_>) -> Result<Self, DataFusionError> {
        let bytes = &proto.record_batches[0];

        let reader = StreamReader::try_new(bytes.as_slice(), None)?;

        // read batches from the reader using the Iterator trait
        let batches = reader.collect::<Result<Vec<RecordBatch>, _>>()?;
        Ok(Self {
            data: batches,
        })
    }
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
                output(StreamItem::RecordBatch(next_batch.clone())).await;
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
