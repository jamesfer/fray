// use std::collections::HashMap;
// use std::pin::Pin;
// use std::sync::Arc;
// use arrow::datatypes::{Schema, SchemaRef};
// use arrow::record_batch::RecordBatch;
// use crossbeam::atomic::AtomicCell;
// use datafusion::error::DataFusionError;
// use futures_util::{FutureExt, StreamExt, TryFutureExt};
// use tokio::sync::{Mutex, RwLock};
// use futures::{Stream, TryFuture, TryStreamExt};
// use futures_util::future::Map;
// use futures_util::stream::{Collect, FusedStream, FuturesOrdered, FuturesUnordered, MapOk};
// use itertools::Itertools;
// use tokio::sync::mpsc::{Receiver, Sender};
// use crate::streaming::action_stream::{Marker, MergedActionStream, OrdinalStreamItem, OrdinalStreamResult, StreamItem, StreamResult};
// use crate::streaming::input_manager::InputManager;
// use crate::streaming::input_reference_mapper::{InputReference, InputReferenceMapper};
// use crate::streaming::operators::task_function::{OperatorFunction, OutputChannel, TaskFunction, TaskState};
// use crate::streaming::task_definition::{OperatorDefinition, OperatorInput, OperatorOutput, TaskDefinition2, TaskInputGen, TaskInputStreamT, TaskInputs};
//
//
// pub struct TaskMain {
//
// }
//
// impl TaskMain {
//     pub async fn init(task_def: TaskDefinition2, initial_inputs: TaskInputs) -> Self {
//
//         TaskMain {
//
//         }
//     }
//
//     pub async fn update_inputs(&self, inputs: TaskInputs) {
//
//     }
//
//
// }
//
// struct TaskGenerationState {
//
// }
//
// impl TaskGenerationState {
//     pub fn new() -> Self {
//         TaskGenerationState {
//
//         }
//     }
//
//     pub async fn add_next_generation(&self, generation: TaskInputGen) {
//         // TODO
//     }
//
//     pub async fn advance_to_latest_generation(&self, ) -> &TaskInputGen {
//         // TODO
//     }
//
//     pub async fn get_wait_state(&self, stream_id: &str, address: &str) -> InputStreamWaitState {
//         // TODO
//     }
// }
//
// // struct PhaseInputStreams {
// //     current_phase: usize,
// //     inputs: Vec<(usize, Vec<ChunkedStream<StreamItem>>)>,
// // }
// //
// // impl PhaseInputStreams {
// //     pub async fn init(
// //         current_phase: usize,
// //         inputs: Vec<(usize, Vec<ChunkedStream<StreamItem>>)>,
// //     ) -> Self {
// //         Self {
// //             current_phase,
// //             inputs,
// //         }
// //     }
// // }
//
// enum OperatorStatus {
//     Running,
//     Finished,
//     Cancelled,
// }
//
// struct OperatorState {
//     definition: OperatorDefinition,
//     function: Box<dyn OperatorFunction>,
//     current_phase: usize,
//     streams: Option<Vec<(usize, Vec<ChunkedStream<StreamItem>>)>>,
//     running_streams: RunningOperatorStreams,
//     status: OperatorStatus,
// }
//
// impl OperatorState {
//     pub fn new(
//         definition: OperatorDefinition,
//         function: Box<dyn OperatorFunction>,
//         // input_reference_mapper: &InputReferenceMapper,
//         // input_manager: &InputManager,
//     ) -> Self {
//         let current_phase = 0;
//         let phase_inputs = &definition.inputs[current_phase].inputs;
//         // let streams = Self::start_input_streams(input_reference_mapper, input_manager, phase_inputs);
//
//         Self {
//             definition,
//             function,
//             current_phase,
//             streams: None,
//             running_streams: RunningOperatorStreams::new(phase_inputs.clone()),
//             status: OperatorStatus::Running,
//         }
//     }
//
//     pub fn is_finished(&self) -> bool {
//         self.status == OperatorStatus::Finished
//     }
//
//     pub async fn cancel(&mut self) {
//         if self.status == OperatorStatus::Running {
//             self.function.cancel().await;
//             self.status == OperatorStatus::Cancelled;
//         }
//     }
//
//     // pub async fn update_references(
//     //     &mut self,
//     //     partition_range: &[usize],
//     //     input_reference_mapper: &InputReferenceMapper,
//     //     input_manager: &InputManager,
//     // ) {
//     //     // TODO Load state
//     //
//     //     self.running_streams.define_streams(
//     //         partition_range,
//     //         input_reference_mapper,
//     //         input_manager,
//     //     ).await;
//     //
//     //     // TODO create output streams
//     // }
//
//     pub async fn update_partitions(&mut self, partition_range: &[usize]) {
//         // TODO
//     }
//
//     pub async fn run_to_marker(
//         &mut self,
//         partition_range: &[usize],
//         input_reference_mapper: &InputReferenceMapper,
//         input_manager: &InputManager,
//     ) {
//         if self.status != OperatorStatus::Running {
//             panic!("Called run on an operator that was not running");
//         }
//
//         // Check if we need to move to the next phase
//         if self.running_streams.all_complete() {
//             if self.definition.inputs.len() <= self.current_phase + 1 {
//                 // This operator is finished because we completed all of the phases
//                 self.finish();
//                 return;
//             }
//
//             // Move to the next phase
//             self.current_phase += 1;
//             self.running_streams = RunningOperatorStreams::new(self.definition.inputs[self.current_phase].inputs.clone());
//             self.running_streams.define_streams(
//                 partition_range,
//                 input_reference_mapper,
//                 input_manager,
//             ).await;
//         }
//
//         // Stream until the next marker
//         let input_streams = self.running_streams.stream_until_next_markers();
//             // streams.iter_mut()
//             // .map(|(ordinal, chunked_streams)| {
//             //     let s = chunked_streams.iter_mut()
//             //         .map(|chunked_stream| {
//             //             chunked_stream.stream_until_marker()
//             //         })
//             //         .collect::<Vec<_>>();
//             //     (*ordinal, s)
//             // })
//             // .collect::<Vec<_>>();
//
//         // TODO this will be fixed when ChunkedStream has the right type
//         //   might need to pinbox the streams though
//         let mut output_stream = self.function.process_streams(&input_streams).await;
//         while let Some(output) = output_stream.next().await {
//             // TODO write output downstream
//         }
//         drop(output_stream);
//
//         // TODO create checkpoint
//         let markers = self.running_streams.get_markers();
//
//         // Check if the operator is exhausted early
//         if self.function.can_finish_phase_early().await {
//             self.finish();
//         }
//     }
//
//     fn finish(&mut self) {
//         // TODO write output downstream
//         let mut output_stream = self.function.finish();
//         self.status = OperatorStatus::Finished;
//     }
//
//     fn initialize_streams(
//         &mut self,
//         partition_range: &[usize],
//         input_reference_mapper: &InputReferenceMapper,
//         input_manager: &InputManager,
//     ) -> &Vec<(usize, Vec<ChunkedStream<StreamItem>>)> {
//         match &self.streams {
//             None => {
//                 // Start streams for the first time
//                 let streams = Self::start_input_streams(
//                     input_reference_mapper,
//                     input_manager,
//                     &self.definition.inputs[self.current_phase].inputs
//                 );
//                 let streams_ref = &streams;
//                 self.streams = Some(streams);
//                 streams_ref
//             }
//             Some(streams) => {
//                 if streams.iter().all(|(_, chunked_streams)| {
//                     chunked_streams.iter().all(|chunked_stream| chunked_stream.is_terminated())
//                 }) {
//                     if self.definition.inputs.len() > self.current_phase + 1 {
//                         self.current_phase += 1;
//                         let new_streams = Self::start_input_streams(
//                             input_reference_mapper,
//                             input_manager,
//                             &self.definition.inputs[self.current_phase].inputs
//                         );
//                         streams = &new_streams;
//                         self.streams = Some(new_streams);
//                     } else {
//                         // TODO finish the operator
//                     }
//                 } else {
//                     // Update each of the input streams based on the new references
//                     for (_, chunked_streams) in streams.iter_mut() {
//                         for chunked_stream in chunked_streams.iter_mut() {
//                             chunked_stream.update(input_manager, input_reference_mapper);
//                         }
//                     }
//                 }
//                 panic!()
//             },
//         }
//     }
//
//     fn start_input_streams(
//         input_reference_mapper: &InputReferenceMapper,
//         input_manager: &InputManager,
//         phase_inputs: &Vec<OperatorInput>,
//     ) -> Vec<(usize, Vec<ChunkedStream<StreamItem>>)> {
//         phase_inputs.iter()
//             .map(|input| {
//                 let chunked_stream = ChunkedStream::start(input.stream_id.clone(), input_manager, input_reference_mapper);
//                 (input.ordinal, chunked_stream)
//             })
//             .collect::<Vec<(usize, Vec<ChunkedStream<StreamItem>>)>>()
//     }
// }
//
// struct RunningStream {
//     reference: InputReference,
//     stream: ChunkedStream<StreamItem>,
// }
//
// struct RunningOperatorStreams {
//     inputs: Vec<OperatorInput>,
//     running_streams: Option<Vec<Vec<RunningStream>>>,
// }
//
// impl RunningOperatorStreams {
//     pub fn new(inputs: Vec<OperatorInput>) -> Self {
//         Self {
//             inputs,
//             running_streams: None,
//         }
//     }
//
//     pub fn all_complete(&self) -> bool {
//         match &self.running_streams {
//             None => false,
//             Some(running_streams) => running_streams.iter()
//                 .flatten()
//                 .all(|running_stream| running_stream.stream.is_terminated()),
//         }
//     }
//
//     pub fn get_markers(&self) -> Vec<Marker> {
//         // TODO
//         vec![]
//     }
//
//     pub fn stream_until_next_markers<'a>(&'a mut self) -> Vec<(usize, Vec<impl Stream<Item=Result<StreamItem, DataFusionError>> + 'a>)> {
//         self.running_streams.as_ref()
//             .unwrap()
//             .iter_mut()
//             .map(|streams| {
//                 streams.iter_mut()
//                     .map(|stream| {
//                         stream.stream.stream_until_marker()
//                     })
//                     .collect::<Vec<_>>()
//             })
//             .zip(self.inputs.iter())
//             .map(|(streams, input)| {
//                 (input.ordinal, streams)
//             })
//             .collect::<Vec<_>>()
//     }
//
//     pub async fn define_streams(
//         &mut self,
//         partition_range: &[usize],
//         input_reference_mapper: &InputReferenceMapper,
//         input_manager: &InputManager,
//     ) {
//         match &mut self.running_streams {
//             None => {
//                 self.running_streams = Some(self.inputs.iter()
//                     .map(|input| {
//                         let chunked_stream = ChunkedStream::start(input.stream_id.clone(), input_manager, input_reference_mapper);
//                         (input.ordinal, chunked_stream)
//                     })
//                     .collect::<Vec<(usize, Vec<ChunkedStream<StreamItem>>)>>().await);
//             },
//             Some(all_running_streams) => {
//                 for (input, existing_streams) in self.inputs.iter().zip(all_running_streams.iter_mut()) {
//                     let references = input_reference_mapper.get_addresses_for(&input.stream_id, partition_range).unwrap();
//
//                     let previous_streams = std::mem::replace(existing_streams, Vec::with_capacity(references.len()));
//                     let mut previous_streams_map = previous_streams.into_iter()
//                         .map(|stream| (stream.reference.clone(), stream))
//                         .collect::<HashMap<_, RunningStream>>();
//
//                     // Replace the existing streams, attempting to reuse them where possible
//                     let mut new_streams_iter = references.into_iter()
//                         .map(|input_reference| {
//                             let maybe_reusable = previous_streams_map.remove(&input_reference);
//                             async move {
//                                 match maybe_reusable {
//                                     // The stream already exists, so we can reuse it
//                                     Some(previous) => previous,
//                                     None => {
//                                         let stream = input_manager.stream_input_no_schema(
//                                             &input_reference.address,
//                                             &input.stream_id,
//                                             0, // TODO properly handle arrays of partitions in input manager
//                                         )
//                                             .await
//                                             .unwrap();
//                                         RunningStream {
//                                             reference: input_reference,
//                                             stream: ChunkedStream::of(stream),
//                                         }
//                                     }
//                                 }
//                             }
//                         })
//                         .collect::<FuturesOrdered<_>>();
//                     while let Some(next) = new_streams_iter.next().await {
//                         existing_streams.push(next);
//                     }
//                 }
//             }
//         }
//     }
// }
//
// struct GenerationMarker;
//
// type OperatorStreamItem = Result<Result<StreamItem, GenerationMarker>, DataFusionError>;
//
// struct IntraTaskChannelManager {
//     channels: HashMap<String, (AtomicCell<Sender<OperatorStreamItem>>, AtomicCell<Receiver<OperatorStreamItem>>)>,
// }
//
// impl IntraTaskChannelManager {
//     const CHANNEL_BUFFER_SIZE: usize = 8;
//
//     pub fn new(
//         stream_ids: impl IntoIterator<Item=String>,
//     ) -> Self {
//         let channels = stream_ids.into_iter()
//             .map(|stream_id| {
//                 let (sender, receiver) = tokio::sync::mpsc::channel(Self::CHANNEL_BUFFER_SIZE);
//                 (stream_id, (AtomicCell::new(sender), AtomicCell::new(receiver)))
//             })
//             .collect();
//         Self { channels }
//     }
//
//     pub fn new_from_operators(operators: &[OperatorDefinition]) -> Self {
//         Self::new(operators.iter()
//             .flat_map(|operator| {
//                 operator.inputs.iter()
//                     .flat_map(|input| input.inputs.iter())
//                     .map(|input| input.stream_id.clone())
//                     .chain(operator.outputs.iter()
//                         .map(|output| output.stream_id.clone()))
//             }))
//     }
//
//     pub fn take_receivers(&self, stream_id: &str, partition_range: &[usize]) -> Option<Vec<Receiver<OperatorStreamItem>>> {
//
//     }
// }
//
// async fn task_main(
//     task: TaskDefinition2,
//     initial_generation: TaskInputGen,
//     task_inputs_generations: Arc<Receiver<TaskInputGen>>,
// ) {
//     let input_manager = Arc::new(InputManager::new(HashMap::new()).await.unwrap());
//     let task_generation_state = Arc::new(TaskGenerationState::new());
//
//     // All the channels to facilitate inter operator communication
//     let intra_task_channel_manager = IntraTaskChannelManager::new_from_operators(&task.operators.operators);
//
//     // TODO Start another task to wait for new input generations
//     // TODO Start each of the input streams in their own task, and write to the channels
//
//     // let mut operators = task.operators.operators.into_iter()
//     //     .map(|operator| {
//     //         let operator_function = operator.spec.clone().create_operator_function();
//     //         (operator, operator_function)
//     //     })
//     //     .collect::<Vec<(OperatorDefinition, Box<dyn OperatorFunction>)>>();
//
//     run_all_generations(task.operators.operators, task_generation_state, &input_manager).await;
//
//     // Start all operators running in the background
//     //  The operators have an inner loop to process streams between markers and take checkpoints
//     //  + an outer loop to process markers between generations and reload the state when it changes
//     // All operators read from channels managed internally
//
//     // Separately, start each of the input streams writing to an internal channel. Streams consume
//     // inputs until they hit a marker, then check if they need to move to the next generation.
// }
//
// async fn run_all_generations(
//     operators: Vec<OperatorDefinition>,
//     task_generation_state: Arc<TaskGenerationState>,
//     input_manager: &InputManager,
// ) {
//
//     let mut operator_states = operators.iter()
//         .map(|operator| {
//             let operator_function = operator.spec.clone().create_operator_function();
//             OperatorState::new(operator.clone(), operator_function)
//         })
//         .collect::<Vec<OperatorState>>();
//
//     while !operator_states.is_empty() {
//         // TODO need the markers of each task
//         let current_generation = task_generation_state.advance_to_latest_generation().await;
//         let input_references = Arc::new(InputReferenceMapper::new(current_generation.streams.clone()));
//
//         // Run all the operators in parallel
//         operator_states = operator_states.into_iter()
//             .map(|mut operator_state| {
//                 tokio::spawn(async move {
//                     operator_state.run_to_marker(&current_generation.partition_range, &input_references, input_manager).await;
//                     operator_state
//                 })
//             })
//             .collect::<FuturesOrdered<_>>()
//             .collect::<Vec<_>>()
//             .into_iter()
//             .collect::<Result<Vec<_>, _>>()
//             .await
//             .unwrap();
//
//         operator_states = removed_finished_operators(operator_states).await;
//     }
// }
//
// async fn operator_main(
//     operator_definition: OperatorDefinition,
//     // partition_range: &[usize],
//     mut function: Box<dyn OperatorFunction>,
//     channels: Arc<IntraTaskChannelManager>
// ) {
//     let mut phase_index = 0;
//     let mut phase_inputs = operator_definition.inputs[phase_index].inputs.clone();
//
//     // Convert each stream to a chunked stream
//     let mut streams = phase_inputs.iter()
//         .map(|input| {
//             let channels = channels.take_receivers(&input.stream_id).unwrap();
//             let streams = channels.into_iter().map(|channel| ChunkedStream::of(channel)).collect::<Vec<_>>();
//             (input.ordinal, streams)
//         })
//         .collect::<Vec<_>>();
//
//     loop {
//         // Stream until the next marker
//         let input_streams = streams.iter_mut()
//             .map(|(ordinal, chunked_stream)| {
//                 (*ordinal, chunked_stream.stream_record_batches())
//             })
//             .collect::<Vec<_>>();
//
//         // TODO fix pin box type
//         let mut output_stream = function.process_streams(&input_streams).await;
//         while let Some(output) = output_stream.next().await {
//             // TODO write output downstream
//         }
//         drop(output_stream);
//
//         // Check if the operator is exhausted early
//         if function.can_finish_phase_early().await {
//             // TODO write output downstream
//             function.finish();
//             return;
//         }
//
//         let markers = extract_current_markers(&mut streams)?.unwrap();
//         if let Ok(markers) = markers {
//             // TODO create checkpoint
//         }
//
//         // Check if we need to move to the next phase
//         let all_streams_finished = streams.iter().all(|(_, chunked_streams)| chunked_streams.iter().all(|chunked_stream| chunked_stream.is_terminated()));
//         if all_streams_finished {
//             if operator_definition.inputs.len() <= phase_index + 1 {
//                 // This operator is finished because we completed all of the phases
//                 // TODO write output downstream
//                 function.finish();
//                 return;
//             }
//
//             // Move to the next phase
//             phase_index += 1;
//             phase_inputs = operator_definition.inputs[phase_index].inputs.clone();
//         }
//
//         // TODO Re-issue streams
//     }
// }
//
// fn extract_current_markers(
//     streams: &mut Vec<(usize, Vec<ChunkedStream>)>,
// ) -> Result<Option<Result<Vec<(usize, Vec<Marker>)>, GenerationMarker>>, DataFusionError> {
//     let nested_markers = streams.iter()
//         .map(|(ordinal, chunked_streams)| {
//             (*ordinal, chunked_streams.iter()
//                 .map(|chunked_stream| chunked_stream.get_marker())
//                 .collect::<Vec<_>>())
//         })
//         .collect::<Vec<_>>();
//
//     nested_markers.into_iter()
//         .map(|(ordinal, markers)| -> Result<_, DataFusionError> {
//             let (marker, generations) = markers.into_iter()
//                 .collect::<Result<Vec<_>, _>>()?
//                 .into_iter()
//                 .map(|x| match x {
//                     Ok(v) => (Some(v), None),
//                     Err(e) => (None, Some(e)),
//                 })
//                 .unzip::<_, _, Vec<_>, Vec<_>>();
//
//             let generation = generations.into_iter()
//                 .flatten()
//                 .try_fold(None, |acc, generation| {
//                     match acc {
//                         None => Ok(Some(generation)),
//                         Some(acc) => {
//                             if acc == generation {
//                                 Ok(Some(acc))
//                             } else {
//                                 Err(DataFusionError::Internal(format!(
//                                     "Encountered out of order generations: {} and {}",
//                                     acc, generation
//                                 )))
//                             }
//                         }
//                     }
//                 })?;
//             let markers = marker.into_iter().flatten().collect::<Vec<_>>();
//
//             match (generation, markers) {
//                 (Some(generation), markers) if markers.is_empty() => {
//                     Ok(Err(generation))
//                 },
//                 (Some(_), _) => {
//                     Err(DataFusionError::Internal("Some streams returned generations, some markers.".to_string()))
//                 },
//                 (None, markers) => {
//                     Ok(Ok((ordinal, markers)))
//                 }
//             }
//         })
//         .try_fold(None, |acc, y| {
//             let b = y?;
//
//             match (acc, b) {
//                 (None, Ok(markers)) => Ok(Some(Ok(vec![markers]))),
//                 (None, Err(generation)) => Ok(Some(Err(generation))),
//                 (Some(Ok(mut markers)), Ok(markers2)) => {
//                     markers.extend(markers2);
//                     Ok(Some(Ok(markers)))
//                 },
//                 (Some(Ok(_markers)), Err(_generation)) => {
//                     Err(DataFusionError::Internal("Some streams returned generations, some markers.".to_string()))
//                 },
//                 (Some(Err(_generation)), Ok(_markers)) => {
//                     Err(DataFusionError::Internal("Some streams returned generations, some markers.".to_string()))
//                 },
//                 (Some(Err(generation)), Err(generation2)) if generation == generation2 => {
//                     Ok(Some(Err(generation)))
//                 },
//                 (Some(Err(_generation)), Err(_generation2)) => {
//                     Err(DataFusionError::Internal("Generations don't match".to_string()))
//                 },
//             }
//         })
// }
//
// // TODO this operator would bug out on circular task graphs, as a downstream operator could be cancelled
// //   causing the upstream one to be permanently blocked since its output is never consumed. Would
// //   need to replace the internal queue between the operators with a "black hole"
// async fn removed_finished_operators(operator_states: Vec<OperatorState>) -> Vec<OperatorState> {
//     let mut all_children_finished = HashMap::new();
//     for operator_state in &operator_states {
//         for input_phase in &operator_state.definition.inputs {
//             for input in &input_phase.inputs {
//                 all_children_finished.entry(input.stream_id.clone())
//                     .and_modify(|all_finished| *all_finished &= operator_state.is_finished())
//                     .or_insert(operator_state.is_finished());
//             }
//         }
//     }
//
//     // Remove all the operators that are finished
//     operator_states.into_iter()
//         .map(|mut operator_state| async move {
//             let can_be_cancelled = operator_state.definition.outputs.iter()
//                 .all(|output| all_children_finished.get(&output.stream_id).copied().unwrap_or(false));
//             if can_be_cancelled {
//                 operator_state.cancel().await;
//                 return None;
//             }
//             Some(operator_state)
//         })
//         .collect::<FuturesOrdered<_>>()
//         .flat_map(futures::stream::iter)
//         .collect::<Vec<_>>()
//         .await
// }
//
// #[derive(Clone)]
// enum InputStreamWaitState {
//     Continue,
//     WaitAsap,
// }
//
// async fn run_all_operators_until_interrupted(
//     operators: Vec<(OperatorDefinition, Box<dyn OperatorFunction>)>,
//     partition_range: &[usize],
//     input_reference_mapper: Arc<InputReferenceMapper>,
//     task_generation_state: Arc<TaskGenerationState>,
//     input_manager: &InputManager,
// ) -> Vec<(OperatorDefinition, Box<dyn OperatorFunction>)> {
//     // Run each of the operators in parallel until we want to finish this generation
//     operators.into_iter()
//         .map(|(def, operator)| tokio::spawn(run_operator_period(
//             def,
//             operator,
//             // TODO can these be references too?
//             input_reference_mapper.clone(),
//             task_generation_state.clone(),
//             input_manager,
//         )))
//         .collect::<FuturesOrdered<_>>()
//         .collect::<Vec<_>>()
//         .await
//         .into_iter()
//         .collect::<Result<Vec<_>, _>>()
//         .unwrap()
// }
//
// async fn run_operator_period(
//     operator_state: &mut OperatorState,
//     input_reference_mapper: Arc<InputReferenceMapper>,
//     task_generation_state: Arc<TaskGenerationState>,
//     input_manager: &InputManager,
// ) {
//     // Perform streaming work here
//     // TODO don't assume that we are in the first phase
//     let phase_inputs = &operator_state.definition.inputs[operator_state.current_phase].inputs;
//
//     // Represents input streams that may terminate early if we want to transition to the next input
//     // generation
//     let input_streams = read_all_inputs(
//         phase_inputs,
//         input_reference_mapper,
//         task_generation_state,
//         input_manager,
//     ).await;
//     let parallel_input_stream = merge_independent_input_streams(input_streams).await;
//
//     // Run the operator until the streams are finished
//     consume_operator_input_stream(parallel_input_stream, &mut operator_state.function).await;
// }
//
// async fn run_operator_until_marker(
//     operator_state: &mut OperatorState,
//     input_reference_mapper: Arc<InputReferenceMapper>,
//     task_generation_state: Arc<TaskGenerationState>,
//     input_manager: &InputManager,
// ) {
//     // Perform streaming work here
//     // TODO don't assume that we are in the first phase
//     let phase_inputs = &operator_state.definition.inputs[operator_state.current_phase].inputs;
//
//     let streams = phase_inputs.iter()
//         .map(|input| {
//             let chunked_stream = ChunkedStream::start(input.stream_id.clone(), input_manager, &input_reference_mapper);
//         })
//
//     // Represents input streams that may terminate early if we want to transition to the next input
//     // generation
//     // let input_streams = read_all_inputs(
//     //     phase_inputs,
//     //     input_reference_mapper,
//     //     task_generation_state,
//     //     input_manager,
//     // ).await;
//     // let parallel_input_stream = merge_independent_input_streams(input_streams).await;
//
//     // Run the operator until the streams are finished
//     consume_operator_input_streams(parallel_input_stream, &mut operator_state.function).await;
// }
//
// async fn read_all_inputs(
//     phase_inputs: &[OperatorInput],
//     input_reference_mapper: Arc<InputReferenceMapper>,
//     task_generation_state: Arc<TaskGenerationState>,
//     input_manager: &InputManager,
// ) -> Vec<(usize, Vec<impl Stream<Item=Result<StreamItem, DataFusionError>>>)> {
//     phase_inputs.iter()
//         .cloned()
//         .map(|input| async {
//             let addresses = input_reference_mapper.get_addresses_for(&input.stream_id).unwrap()
//                 .iter()
//                 .map(|address| read_input_until_interrupted(
//                     address,
//                     &input.stream_id,
//                     task_generation_state.clone(),
//                     input_manager,
//                 ))
//                 .collect::<FuturesUnordered<_>>()
//                 .collect::<Vec<_>>()
//                 .await;
//             (input.ordinal, addresses)
//         })
//         .collect::<FuturesUnordered<_>>()
//         .collect::<Vec<_>>()
//         .await
// }
//
// struct ChunkedStream {
//
// }
//
// impl <T> FusedStream for ChunkedStream {
//     fn is_terminated(&self) -> bool {
//         // TODO
//         false
//     }
// }
//
// impl ChunkedStream {
//     // pub fn start(stream_id: String, input_manager: &InputManager, references: &InputReferenceMapper) -> Self {
//     //     ChunkedStream {
//     //
//     //     }
//     // }
//
//     pub fn of(stream: impl Stream<Item=OperatorStreamItem>) -> Self {
//
//     }
//
//     pub fn stream_record_batches<'a>(&'a mut self) -> impl Stream<Item=RecordBatch> + 'a {
//
//     }
//
//     pub fn get_marker(&self) -> Result<Result<Marker, GenerationMarker>, DataFusionError> {
//
//     }
// }
//
// enum StreamSignal<T> {
//     Continue(T),
//     Stop,
// }
//
// impl <T> StreamSignal<T> {
//     fn get_value(&self) -> Option<T> {
//         match self {
//             StreamSignal::Continue(value) => Some(value),
//             StreamSignal::Stop => None
//         }
//     }
// }
//
// // TODO how to handle intra task communication
// async fn read_input_until_interrupted(
//     address: &str,
//     stream_id: &str,
//     task_generation_state: Arc<TaskGenerationState>,
//     input_manager: &InputManager,
// ) -> impl Stream<Item=StreamResult> + Sized {
//     // TODO input partition
//     input_manager.stream_input_no_schema(address, stream_id, 0).await?
//         .map(|result| {
//             async {
//                 match result {
//                     Err(error) => {
//                         Err(error)
//                     },
//                     Ok(StreamItem::Marker(marker)) => {
//                         // Check if we need to stop at this marker
//                         match task_generation_state.get_wait_state(stream_id, address).await {
//                             InputStreamWaitState::Continue => {
//                                 // Pass the marker downstream and then do nothing
//                                 Ok(vec![StreamSignal::Continue(StreamItem::Marker(marker))])
//                             },
//                             InputStreamWaitState::WaitAsap => {
//                                 // Pass the marker downstream and then finish the stream early
//                                 Ok(vec![StreamSignal::Continue(StreamItem::Marker(marker)), StreamSignal::Stop])
//                             }
//                         }
//                     },
//                     Ok(StreamItem::RecordBatch(record_batch)) => {
//                         // Pass the record batch down stream
//                         Ok(vec![StreamSignal::Continue(StreamItem::RecordBatch(record_batch))])
//                     },
//                 }
//             }
//         })
//         .flat_map(|future| {
//             future.map_ok_or_else(
//                 |err| vec![Err(err)],
//                 |items| items.into_iter().map(|x| Ok(x)).collect::<Vec<_>>(),
//             ).into_stream()
//         })
//         .flat_map(|x| futures::stream::iter(x))
//         .take_until(|x| futures::future::ready(matches!(x, Ok(None))))
//         .filter_map(|x| x.map(StreamSignal::get_value).transpose())
// }
//
// async fn merge_independent_input_streams(
//     streams: Vec<(usize, Vec<impl Stream<Item=Result<StreamItem, DataFusionError>>>)>,
// ) -> impl Stream<Item=Result<Result<(usize, usize, RecordBatch), Vec<(usize, Marker)>>, DataFusionError>> {
//     // TODO
// }
//
// async fn consume_operator_input_streams(
//     operator_function: &mut Box<dyn OperatorFunction>,
//     // TODO should these be in a Pin::Box
//     input_streams: &mut [(usize, Vec<ChunkedStream<RecordBatch>>)],
// ) -> Vec<(usize, Vec<Marker>)>{
//     let mut output_stream = operator_function.process_streams(input_streams).await;
//
//     // Write output stream downstream
//     while let Some(output) = output_stream.next().await {
//         // TODO
//     }
//     drop(output_stream);
//
//     let markers = input_streams.iter()
//         .map(|(ordinal, streams)| {
//             (*ordinal, streams.iter()
//                 .map(|stream| stream.get_marker())
//                 .collect::<Vec<_>>())
//         })
//         .collect::<Vec<_>>();
//     // TODO Write checkpoint
//     markers
// }
//
// async fn consume_operator_input_stream(
//     mut parallel_input_stream: impl Stream<Item=Result<Result<(usize, usize, RecordBatch), Vec<(usize, Marker)>>, DataFusionError>>,
//     operator_function: &mut Box<dyn OperatorFunction>,
// ) {
//     while true {
//         // Split the stream into data and markers
//         let (data_stream, next_marker_future) = split_input_stream_on_markers(parallel_input_stream).await;
//
//         // Feed the data into the operator function
//         let mut output_stream = operator_function.process_stream(data_stream).await;
//
//         // Write output stream downstream
//         while let Some(output) = output_stream.next().await {
//             // TODO
//         }
//         drop(output_stream);
//
//         let (markers, remaining_stream) = next_marker_future.await;
//         parallel_input_stream = remaining_stream;
//
//         // TODO save state in checkpoint handler
//         operator_function.get_state().await;
//     }
//
//     // while let Some(combined_result) = parallel_input_stream.next().await {
//     //     match combined_result {
//     //         Ok(Ok((ordinal, record_batch))) => {
//     //             let output_stream = operator_function.process(record_batch, ordinal).await;
//     //         },
//     //         Ok(Err(markers)) => {
//     //             // TODO save state in checkpoint handler
//     //             operator_function.get_state().await;
//     //         }
//     //         Err(err) => {
//     //             // TODO handle error
//     //             panic!("Error in input stream: {:?}", err);
//     //         },
//     //     }
//     // }
// }
//
// async fn split_input_stream_on_markers<S, T>(
//     parallel_input_stream: S,
// ) -> (impl Stream<Item=T>, impl Future<Output=(Vec<(usize, Marker)>, S)>)
// where S: Stream<Item=Result<Result<T, Vec<(usize, Marker)>>, DataFusionError>>
// {
// }
//
//
// // trait PartialStream: Stream {
// //     type ResidualItem;
// //
// //     fn
// // }
//
// // async fn feed_record_batch(
// //     operator_id: String,
// //     mut function: Box<dyn OperatorFunction>,
// //     record_batch: RecordBatch,
// //     ordinal: usize,
// //     graph: HashMap<String, Vec<(String, usize)>>,
// //     mut operator_states: HashMap<String, OperatorState>,
// // ) {
// //     // let (outputs, task_state) = function.process(record_batch, ordinal).await;
// //
// //     let mut ready = vec![operator_id.clone()];
// //     let mut finishing = vec![];
// //     // Pull and item off of the queue
// //     // Process some of it's input
// //     // Send the output downstream if there is any
// //     // Update the state of each of the tasks involved
// //
// //     while let Some(next) = ready.pop() {
// //         let downstream_outputs = {
// //             let state = operator_states.get_mut(&next).unwrap();
// //             match step_one_input(state) {
// //                 None => {
// //                     if state.input_queues.iter().all(|(marker, _)| marker.is_some()) {
// //                         // Operator is ready for checkpointing
// //                     } else {
// //                         // Operator has no more inputs to process for now
// //                     }
// //                     None
// //                 }
// //                 Some((downstream_outputs, task_state)) => {
// //                     if task_state == TaskState::Exhausted {
// //                         finishing.push(next);
// //                     } else if state.input_queues.iter().all(|(marker, _)| marker.is_some()) {
// //                         // Operator is ready for checkpointing
// //                     } else if state.input_queues.iter().all(|(marker, queue)| marker.is_none() && !queue.is_empty()) {
// //                         // Operator still has some inputs that can be processed
// //                         ready.push(next);
// //                     } else {
// //                         // Operator has no more inputs to process for now
// //                     }
// //
// //                     Some(downstream_outputs)
// //                 }
// //             }
// //         };
// //
// //         // Store downstream outputs in downstream states
// //         // TODO handle output channels
// //         // TODO get output id from operator function
// //         let output_id = "abc";
// //         let downstream_channels = graph.get(&output_id).cloned().unwrap_or_default();
// //         for (downstream_id, downstream_ordinal) in downstream_channels {
// //             let downstream_state = operator_states.get_mut(&downstream_id).unwrap();
// //             let downstream_input_queue = &mut downstream_state.input_queues[downstream_ordinal];
// //             downstream_input_queue.1.extend(downstream_outputs.clone());
// //         }
// //     }
// //
// //
// //     // Look up the downstream operators
// //     match graph.get(&operator_id) {
// //         None => {}
// //         Some(downstreams) => {
// //             for ((downstream_id, downstream_ordinal), outputs) in downstreams.iter().zip(vec![outputs; downstreams.len()]) {
// //                 let downstream_state = operator_states.get_mut(downstream_id).unwrap();
// //                 let downstream_input_queue = &mut downstream_state.input_queues[*downstream_ordinal];
// //                 downstream_input_queue.1.extend(outputs.clone());
// //             }
// //
// //             // Just return downstream ids
// //             downstreams.iter().map(|(downstream_id, _)| downstream_id.clone()).collect()
// //         }
// //     }
// // }
// //
// // fn step_one_input(
// //     state: &mut OperatorState,
// // ) -> Option<(Vec<StreamItem>, TaskState)> {
// //     // Check each of the input channels
// //     for (input_ordinal, (maybe_marker, input_queue)) in state.input_queues.iter_mut().enumerate() {
// //         if maybe_marker.is_some() {
// //             // Skip
// //             continue;
// //         }
// //
// //         if let Some(next_item) = input_queue.pop() {
// //             match next_item {
// //                 StreamItem::Marker(marker) => {
// //                     // Store the marker, so we know this input stream is blocked until we can
// //                     // create a state checkpoint
// //                     *maybe_marker = Some(marker);
// //                 }
// //                 StreamItem::RecordBatch(record_batch) => {
// //                     return Some(state.function.process(record_batch, input_ordinal));
// //                 }
// //             }
// //         }
// //     }
// //
// //     None
// //
// //     // // When there are no more inputs, return clear
// //     // (vec![], OperatorProcessingState::Clear)
// // }
//
// // enum OperatorProcessingState {
// //     Exhausted,
// //     MoreInputs,
// //     AllBlocked,
// //     Clear,
// // }
//
// // struct OperatorState {
// //     function: Box<dyn OperatorFunction>,
// //     // Width of the number of input channels
// //     input_queues: Vec<(Option<Marker>, Vec<StreamItem>)>,
// // }
