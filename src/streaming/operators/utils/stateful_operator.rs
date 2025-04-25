// use std::pin::Pin;
// use arrow::ipc::RecordBatch;
// use async_trait::async_trait;
// use futures::Stream;
// use crate::streaming::operators::task_function::{CreateOperatorFunction2, OperatorFunction2, SItem};
// use crate::streaming::operators::utils::chunked_stream::ChunkedStream;
//
// #[async_trait]
// pub trait StatefulOperator {
//     async fn init(&mut self);
//     async fn process<'a>(
//         &'a mut self,
//         inputs: Vec<(usize, &'a Vec<&'a mut Pin<Box<dyn Stream<Item=RecordBatch>>>>)>,
//     ) -> Vec<(usize, &'a Vec<&'a mut Pin<Box<dyn Stream<Item=RecordBatch> + 'a>>>)>;
//     async fn make_checkpoint(&mut self);
//     async fn load_checkpoint(&mut self, checkpoint: u64, partitions: Vec<usize>);
//     async fn close(self);
// }
//
// #[async_trait]
// pub trait RecordStateOperator {
//     async fn init(&mut self);
//     async fn process<'a>(
//         &'a mut self,
//         inputs: Vec<(usize, &'a Vec<&'a mut Pin<Box<dyn Stream<Item=RecordBatch>>>>)>,
//     ) -> Vec<(usize, &'a Vec<&'a mut Pin<Box<dyn Stream<Item=RecordBatch> + 'a>>>)>;
//     async fn get_state(&mut self) -> RecordBatch;
//     async fn use_state(&mut self, state: RecordBatch);
//     async fn close(self);
// }
//
// #[async_trait]
// pub trait StatelessOperator {
//     async fn init(&mut self);
//     async fn process<'a>(
//         &'a mut self,
//         inputs: Vec<(usize, &'a Vec<&'a mut Pin<Box<dyn Stream<Item=RecordBatch>>>>)>,
//     ) -> Vec<(usize, &'a Vec<&'a mut Pin<Box<dyn Stream<Item=RecordBatch> + 'a>>>)>;
//     async fn close(self);
// }
//
// pub trait CreateStatelessOperatorFunction {
//     fn create_stateless_operator_function(&self) -> Box<dyn StatelessOperator + Sync + Send>;
// }
//
// impl CreateOperatorFunction2 for dyn CreateStatelessOperatorFunction {
//     fn create_operator_function(&self) -> Box<dyn OperatorFunction2 + Sync + Send> {
//         Box::new(StatelessOperatorWrapper {
//             function: self.create_stateless_operator_function(),
//         })
//     }
// }
//
// struct StatelessOperatorWrapper {
//     function: Box<dyn StatelessOperator + Sync + Send>,
// }
//
// #[async_trait]
// impl OperatorFunction2 for StatelessOperatorWrapper {
//     async fn init(&mut self) {
//         self.function.init()
//     }
//
//     async fn run<'a>(
//         &'a mut self,
//         inputs: Vec<(usize, Vec<Pin<Box<dyn Stream<Item=SItem> + Send + Sync>>>)>,
//     ) -> Vec<(usize, Vec<Pin<Box<dyn Stream<Item=SItem> + Send + Sync>>>)> {
//         // Convert each of the inputs into a chunked stream
//         let mut chunked_inputs = inputs.into_iter()
//             .map(|(ordinal, streams)| {
//                 let chunked = streams.into_iter()
//                     .map(|stream| ChunkedStream::of(stream))
//                     .collect::<Vec<_>>();
//                 (ordinal, chunked)
//             })
//             .collect::<Vec<_>>();
//
//         let chunks = chunked_inputs.iter_mut()
//             .map(|(ordinal, streams)| {
//                 let mut stream = streams.iter_mut()
//                     .map(|stream| stream.stream_record_batches())
//                     .collect::<Vec<_>>();
//                 (ordinal, stream)
//             })
//             .collect::<Vec<_>>();
//
//         // Call the operator function with the chunks
//         // TODO fix types
//         let output = self.function.process(chunks).await;
//
//         // TODO Now that we know how many outputs there are, we should call the lazy function
//
//         // Write the outputs to the output streams
//         // TODO
//
//         // Since the operator is stateless, we just create an empty checkpoint
//         // TODO
//         let this_marker = 123u64;
//
//         // Forward the next marker downstream
//
//         ()
//     }
//
//     async fn close(self) {
//         self.function.close()
//     }
// }
