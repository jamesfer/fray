use std::fmt::{Debug, Formatter};
use crate::streaming::action_stream::{Marker, OrdinalStreamResult, SendableActionStream, StreamItem};
use async_trait::async_trait;
use datafusion::arrow::record_batch::RecordBatch;
use std::future::Future;
use std::pin::Pin;
use datafusion::common::DataFusionError;
use futures::Stream;

pub type OutputChannel = OutputChannelL<'static>;
pub type OutputChannelL<'a> = Box<dyn (FnMut(StreamItem) -> Pin<Box<dyn Future<Output=()> + Sync + Send + 'a>>) + Sync + Send + 'a>;

// TODO rename to operator function
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

#[async_trait]
pub trait OperatorFunction {
    async fn init(&mut self);
    async fn process_streams<'a>(&'a mut self, input_streams: &'a mut [(usize, Vec<Pin<Box<dyn Stream<Item=RecordBatch> + 'a>>>)]) -> Pin<Box<dyn Stream<Item=OrdinalStreamResult> + 'a>>;
    async fn process_stream<'a>(&'a mut self, input: Pin<Box<dyn Stream<Item=(usize, RecordBatch)> + 'a>>) -> Pin<Box<dyn Stream<Item=OrdinalStreamResult> + 'a>>;
    // async fn process<'a>(&'a mut self, input: RecordBatch, ordinal: usize) -> Pin<Box<dyn Stream<Item=OrdinalStreamResult> + 'a>>;
    async fn can_finish_phase_early(&mut self) -> bool;
    // async fn finish_phase(&mut self, phase_index: usize) -> Pin<Box<dyn Stream<Item=OrdinalStreamResult>>>;
    async fn increment_phase(&mut self, phase_index: usize);
    // Finish is called when the operator is done processing all input streams, or it voluntarily
    // says that it no longer needs any more input
    async fn finish(&mut self);
    // Cancel is called when the whole query cancels, or all downstream operators finished early
    async fn cancel(&mut self);
    async fn get_state(&mut self) -> RecordBatch;
    async fn load_state(&mut self, state: RecordBatch);
}


pub enum SItem {
    RecordBatch(RecordBatch),
    Marker(Marker),
    Generation(usize),
}

#[async_trait]
pub trait OperatorFunction2 {
    async fn init(&mut self);
    // Source operators would have no inputs defined, so this would be an empty slice
    // The majority of operators would only take a single input stream per ordinal, and only return
    // a single output per ordinal (and the majority of those would only return one output stream).
    // The vectors allow for operators to make use of the preserved ordered-ness of the inputs
    // steams when they come from multiple different operators.
    // Output operators would have no outputs defined, as they would publish their results over the
    // network. They would return a future that eventually completes with an empty array.
    // When a marker arrives in the input stream, it's the operators responsibility to take
    // checkpoints at appropriate intervals. This functionality could be wrapped in some kind of
    // common class so the majority of operators don't need to worry about it.
    // When a generation marker is received, the operator should take care to download the necessary
    // state partitions.
    // Regular markers can be consumed in any order, as long as all the streams are at a defined
    // marker when checkpoints are taken. However, generation markers can only be consumed when all
    // streams are at the same marker. Once a generation marker appears in one stream, other streams
    // must be consumed until they area also at the same generation point, then the new state can be
    // downloaded and the markers passed downstream.
    // Sometimes, and operator will need to jump to a different marker and or partition offset
    // immediately, such as when an upstream task fails, and the coordinator tells this task to
    // reset to the previous checkpoint and consume from the replacement task. When this occurs,
    // the runtime will drop the returned future and streams and start the run call again with a
    // different marker.
    // It is legal for tasks to only consume some of their inputs at a time, such as a join operator
    // that needs to consume the build side entirely, before consuming the probe side. The runtime
    // ensures that upstream operators are run lazily, so they won't start consuming or producing
    // results until all of their output streams are being awaited. This is necessary so that when
    // the probe side task starts being consumed in the example above, it uses the most recent
    // generation marker, rather than the marker that the task started with. In turn, the operator
    // agrees that once it polls a stream, it must support consuming its contents at some point
    // before processing a generation marker.
    // TODO handle errors
    async fn run<'a>(
        &'a mut self,
        inputs: Vec<(usize, Vec<Pin<Box<dyn Stream<Item=SItem> + Send + Sync>>>)>,
    ) -> Vec<(usize, Vec<Pin<Box<dyn Stream<Item=SItem> + Send + Sync>>>)>;
    async fn close(self: Box<Self>);
}

pub trait CreateOperatorFunction2 {
    fn create_operator_function(&self) -> Box<dyn OperatorFunction2 + Sync + Send>;
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
