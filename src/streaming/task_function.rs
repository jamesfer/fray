use std::future::Future;
use std::pin::Pin;
use async_trait::async_trait;
use datafusion::arrow::record_batch::RecordBatch;
use tonic::async_trait;
use crate::streaming::action_stream::{Marker, StreamItem};

pub type OutputChannel = OutputChannelL<'static>;
pub type OutputChannelL<'a> = Box<dyn (FnMut(StreamItem) -> Pin<Box<dyn Future<Output=()> + Sync + Send + 'a>>) + Sync + Send + 'a>;

#[async_trait]
pub trait TaskFunction {
    async fn init(&mut self);
    async fn poll(&mut self, output: &mut OutputChannelL) -> TaskState;
    async fn process(&mut self, data: RecordBatch, input_channel: usize, output: &mut OutputChannel) -> TaskState;
    // TODO should this consume self
    async fn finish(&mut self, output: &mut OutputChannel);
    async fn get_state(&mut self) -> RecordBatch;
    async fn load_state(&mut self, state: RecordBatch);
}

#[derive(Debug, PartialEq)]
pub enum TaskState {
    Continue,
    Exhausted,
}

// Task lifecycle
// init()
// configure()
// load_state()
// process() (repeating)
// get_state()
// configure() when certain task parameters change
// load_state()
// process()
// get_state()
// finish()

// Task reads non-partitioned input, there is only one task instance, and one state
// Task reads hash-partitioned input, has state tied to that key
// Task reads round-robin input, uses fake partitions based on a counter

// Round-robin input creates fake partitions based on an auto-incrementing counter.
// OR
// Mergeable state isn't tied to a particular partition of the input, it can be used by any task,
// as long as it is only used once. Sorting is a good example of an operator that would use
// mergeable state. There would be many task instances trying to sort parts of the data in parallel,
// storing their partially sorted results in the state. If the number of tasks change, the state can
// be used as the starting point of any new task, as long as it is only used once.



// pub trait TaskFactory {
//     async fn init(&mut self) -> Box<dyn TaskRuntimeFunction>;
// }
//
// pub trait TaskRuntimeFunction {
//     async fn process(self, data: RecordBatch, output: OutputChannel) -> TaskRuntimeFunctionProcessOutput<Self>;
//     async fn get_state(&mut self) -> RecordBatch;
//     async fn load_state(&mut self, state: RecordBatch);
// }
//
// pub trait TaskSourceFunction {
//     async fn poll(self, output: OutputChannel) -> TaskRuntimeFunctionProcessOutput<Self>;
// }
//
// pub enum TaskRuntimeFunctionProcessOutput<T> {
//     Done,
//     Continue(T),
// }
