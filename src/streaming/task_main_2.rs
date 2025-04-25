// use std::collections::hash_map::Entry;
// use std::collections::HashMap;
// use std::sync::Arc;
// use arrow::array::RecordBatch;
// use datafusion::common::{internal_datafusion_err, DataFusionError};
// use futures::{Stream, TryFuture};
// use futures_util::future::TryJoinAll;
// use futures_util::stream::FusedStream;
// use futures_util::{StreamExt, TryFutureExt};
// use tokio::sync::mpsc::{Receiver, Sender};
// use tokio::task::{JoinError, JoinHandle};
// use crate::streaming::action_stream::{Marker, OrdinalStreamResult, StreamItem};
// use crate::streaming::operators::task_function::OperatorFunction;
// use crate::streaming::task_definition::{OperatorDefinition, OperatorInput};
//
// struct GenerationMarker;
//
// type OperatorStreamItem = Result<Result<StreamItem, GenerationMarker>, DataFusionError>;
//
// struct IntraTaskChannelManager;
//
// impl IntraTaskChannelManager {
//     fn take_sender(&self, stream_id: &str) -> Result<Sender<OperatorStreamItem>, String> {
//
//     }
//
//     fn take_receivers(&self, stream_id: &str) -> Result<Vec<Receiver<OperatorStreamItem>>, String> {
//     }
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
// async fn task_main(
//     operators: Vec<OperatorDefinition>,
// ) -> Result<(), DataFusionError>{
//     let channels = Arc::new(IntraTaskChannelManager {});
//
//     // // Map between operator id and the phase its downstream operators must be in for it to run
//     // let mut operator_dependencies = HashMap::<String, Vec<(String, usize)>>::new();
//     // for operator in &operators {
//     //     for (index, phase) in operator.inputs.iter().enumerate() {
//     //         for input in &phase.inputs {
//     //             let o = operator.id.clone();
//     //             let i = input.stream_id.clone();
//     //             operator_dependencies.insert(phase.stream_id.clone(), operator.operator_id.clone());
//     //         }
//     //     }
//     // }
//
//     // Run all operators
//     let x = operators.iter()
//         .map(|operator| start_operator(operator, 0, channels.clone()))
//         .collect::<TryJoinAll<_>>()
//         .await?;
//
//
//     Ok(())
// }
//
// #[derive(Debug, Clone, PartialEq)]
// enum OperatorPhaseStatus {
//     Waiting,
//     Running { phase: usize },
//     Cancelled,
//     Finished,
// }
//
// struct OperatorDependencies {
//     downstream_dependencies: Vec<(String, usize)>,
//     operator_definition: OperatorDefinition,
// }
//
// async fn operator_starter_task(
//     operators: Vec<OperatorDefinition>,
//     operator_starting_phases: HashMap<String, usize>,
//     operator_states: HashMap<String, OperatorPhaseStatus>,
//     // operator_dependencies: HashMap<String, Vec<(String, usize)>>,
//     // operator_states_channel: Receiver<(String, usize)>,
// ) {
//     //
// }
//
// enum OperatorAction {
//     Start,
//     Stop,
// }
//
// // async fn on_state_change(
// //     // current_operator_phases: HashMap<String, usize>,
// //     operator_dependencies: &HashMap<String, OperatorDependencies>,
// //     stream_id_lookup: &HashMap<String, (String, String)>,
// //
// //     operator_states: &mut HashMap<String, OperatorPhaseStatus>,
// //     desired_operator_states: &mut HashMap<String, OperatorPhaseStatus>,
// //     state_change: (String, OperatorPhaseStatus),
// //     channels: Arc<IntraTaskChannelManager>,
// // ) -> Result<(), DataFusionError> {
// //     let mut changes_to_check = vec![state_change];
// //     let mut actions_to_take = HashMap::<String, OperatorAction>::new();
// //
// //     // Using the graph of dependencies, check which new operators can be started and which need to
// //     // be stopped
// //     while let Some(change) = changes_to_check.pop() {
// //         let (operator_id, state) = change;
// //         match operator_states.entry(operator_id.clone()) {
// //             Entry::Occupied(mut existing) => {
// //                 // The state of the operator didn't change
// //                 if existing.get() == &state {
// //                     continue;
// //                 }
// //
// //                 // If the state changed, update the entry, and check if of the upstream operators
// //                 // need to change state
// //                 let previous = existing.insert(state.clone());
// //
// //                 let desired = desired_operator_states.get(&operator_id)
// //                     .ok_or(internal_datafusion_err!("Operator {} not found", operator_id))?;
// //                 if desired == &state {
// //                     // The new state is matching the desired one, so there is no action to take
// //                     continue;
// //                 }
// //
// //
// //                 let dependencies = operator_dependencies.get(&operator_id).ok_or(internal_datafusion_err!("Operator {} not found", operator_id))?;
// //                 let upstream_operators_to_check = [previous, state].iter()
// //                     .flat_map(|status| {
// //                         match status {
// //                             OperatorPhaseStatus::Running { phase } => Some(phase),
// //                             _ => None,
// //                         }
// //                     })
// //                     .flat_map(|phase| dependencies.operator_definition.inputs[*phase].inputs.iter().map(|input| input.stream_id.clone()))
// //                     .map(|stream_id| {
// //                         stream_id_lookup.get(&stream_id)
// //                             .map(|(source_operator_id, _)| source_operator_id.clone())
// //                             .ok_or(internal_datafusion_err!("Stream {} not found", stream_id))
// //                     })
// //                     .collect::<Result<Vec<_>, _>>()?;
// //                 // let downstream_operators_to_check = dependencies.operator_definition.outputs.iter()
// //                 //     .map(|output| {
// //                 //         stream_id_lookup.get(&output.stream_id)
// //                 //             .map(|(_, destination_operator_id)| destination_operator_id.clone())
// //                 //             .ok_or(internal_datafusion_err!("Stream {} not found", output.stream_id))
// //                 //     })
// //                 //     .collect::<Result<Vec<_>, _>>()?;
// //
// //                 for upstream_operator_id in upstream_operators_to_check {
// //                     // Check if the upstream operator should run
// //                     let operator = operator_dependencies.get(&upstream_operator_id)
// //                         .ok_or(internal_datafusion_err!("Operator {} not found", upstream_operator_id))?;
// //                     let current_state = operator_states.get(&upstream_operator_id)
// //                         .ok_or(internal_datafusion_err!("Operator {} not found", upstream_operator_id))?;
// //                     match (current_state, operator_should_run(operator, operator_states)?) {
// //                         (OperatorPhaseStatus::Waiting, true) => {
// //                             // The operator should be started
// //                             actions_to_take.insert(upstream_operator_id.clone(), OperatorAction::Start);
// //                         },
// //                         (OperatorPhaseStatus::Waiting, false) => {
// //                             // Nothing to do
// //                         },
// //                         (OperatorPhaseStatus::Running { .. }, true) => {
// //                             // Nothing to do
// //                         },
// //                         (OperatorPhaseStatus::Running { .. }, false) => {
// //                             // The operator should be stopped, but this most likely occurs when the
// //                             // operator is about to tell us it is stopped anyway, so we just don't
// //                             // do anything for now.
// //                             // return Err(internal_datafusion_err!("Operator {} should be stopped, but is still running. We don't know how to handle this yet", upstream_operator_id));
// //                         },
// //                         (OperatorPhaseStatus::Finished, false) => {
// //                             // Nothing to do
// //                         },
// //                         (OperatorPhaseStatus::Cancelled, false) => {
// //                             // Nothing to do
// //                         },
// //                         (OperatorPhaseStatus::Finished, true) => {
// //                             // Nothing to do, since operators cannot be restarted after they are finished
// //                         },
// //                         (OperatorPhaseStatus::Cancelled, true) => {
// //                             // Nothing to do, since operators cannot be restarted after they are finished
// //                         },
// //                     }
// //                 }
// //             }
// //             Entry::Vacant(_) => {
// //                 return Err(internal_datafusion_err!("Operator {} not found", operator_id));
// //             }
// //         }
// //     }
// //
// //     // Now we have a list of all the changes to operators that need to be done, we act on them
// //     for (operator_id, action) in actions_to_take {
// //         match action {
// //             OperatorAction::Start => {
// //                 let operator = operator_dependencies.get(&operator_id)
// //                     .ok_or(internal_datafusion_err!("Operator {} not found", operator_id))?;
// //
// //                 // The start operator method creates a new task
// //                 start_operator(
// //                     operator.operator_definition.clone(),
// //                     0,
// //                     channels.clone(),
// //                 ).await
// //             },
// //             OperatorAction::Stop => {
// //                 return Err(internal_datafusion_err!("Operator {} should be stopped, but we don't know how to do that yet", operator_id));
// //             },
// //         }
// //     }
// //
// //     Ok(())
// //
// // }
// //
// // fn operator_should_run(
// //     operator: &OperatorDependencies,
// //     operator_states: &HashMap<String, OperatorPhaseStatus>,
// // ) -> Result<bool, DataFusionError> {
// //     Ok(operator.downstream_dependencies.iter()
// //         .map(|(operator, desired_phase)| {
// //             let downstream_status = operator_states.get(operator)
// //                 .ok_or(internal_datafusion_err!("Operator {} not found", operator))?;
// //             Ok(matches!(downstream_status, OperatorPhaseStatus::Running { phase } if phase == desired_phase))
// //         })
// //         .collect::<Result<Vec<_>, _>>()?
// //         .iter()
// //         .all(|x| *x))
// // }
//
// async fn start_operator(
//     operator_definition: &OperatorDefinition,
//     starting_phase: usize,
//     channels: Arc<IntraTaskChannelManager>,
//     // state_channel: Sender<OperatorPhaseStatus>,
// ) -> Result<(), DataFusionError> {
//     let handle: impl Future<Output=Result<Result<(), DataFusionError>, JoinError>> = tokio::spawn(async move {
//         let mut function = operator_definition.spec.create_operator_function();
//         function.init().await;
//         operator_main(operator_definition, function, starting_phase, channels).await
//     });
//     match handle.await {
//         Ok(Ok(())) => Ok(()),
//         Ok(Err(err)) => Err(err),
//         Err(join_error) => Err(internal_datafusion_err!("Failed to join operator task: {}", join_error)),
//     }
// }
//
// async fn operator_main(
//     operator_definition: &OperatorDefinition,
//     mut function: Box<dyn OperatorFunction>,
//     starting_phase: usize,
//     channels: Arc<IntraTaskChannelManager>,
// ) -> Result<(), DataFusionError> {
//     match run_all_operator_phases(operator_definition, &mut function, channels, starting_phase).await {
//         Ok(_) => {
//             function.finish().await;
//             Ok(())
//         },
//         Err(err) => {
//             function.cancel().await;
//             Err(err)
//         },
//     }
// }
//
// async fn run_all_operator_phases(
//     operator_definition: &OperatorDefinition,
//     mut function: &mut Box<dyn OperatorFunction>,
//     channels: Arc<IntraTaskChannelManager>,
//     starting_phase: usize
// ) -> Result<(), DataFusionError> {
//     for (offset, phase_inputs) in operator_definition.inputs[starting_phase..].iter().enumerate() {
//         // Convert each stream to a chunked stream
//         let mut input_streams = phase_inputs.iter()
//             .map(|input| {
//                 let channels = channels.take_receivers(&input.stream_id).unwrap();
//                 let streams = channels.into_iter().map(|channel| ChunkedStream::of(channel)).collect::<Vec<_>>();
//                 (input.ordinal, streams)
//             })
//             .collect::<Vec<_>>();
//
//         let output_senders = operator_definition.outputs.iter()
//             .map(|output| {
//                 let sender = channels.take_sender(output.stream_id.as_str()).unwrap();
//             })
//
//         // Loop through all of the messages in this generation
//         run_operator_until_break(&mut function, &mut input_streams).await?;
//     }
//     Ok(())
// }
//
// async fn run_operator_until_break(
//     function: &mut Box<dyn OperatorFunction>,
//     mut streams: &mut Vec<(usize, Vec<ChunkedStream>)>
// ) -> Result<(), DataFusionError> {
//     while {
//         let all_streams_finished = streams.iter()
//             .all(|(_, chunked_streams)| chunked_streams.iter().all(|chunked_stream| chunked_stream.is_terminated()));
//         !all_streams_finished && !function.can_finish_phase_early().await
//     } {
//         // Stream until the next marker
//         let input_streams = streams.iter_mut()
//             .map(|(ordinal, chunked_streams)| {
//                 let streams = chunked_streams.iter_mut()
//                     .map(|chunked_stream| chunked_stream.stream_record_batches())
//                     .collect::<Vec<_>>();
//                 (*ordinal, streams)
//             })
//             .collect::<Vec<_>>();
//
//         // Run the function
//         // TODO fix pin box type
//         let mut output_stream = function.process_streams(&input_streams).await;
//         while let Some(output) = output_stream.next().await {
//             // TODO write output downstream
//         }
//         drop(output_stream);
//
//         // Take a checkpoint after the marker
//         let markers = extract_current_markers(streams)?.unwrap();
//         let remaining_marker = match markers {
//             Ok(markers) => {
//                 // TODO create checkpoint
//                 None
//             },
//             Err(err) => {
//                 // TODO load state
//                 Some(err)
//             },
//         };
//     }
//
//     Ok(())
// }
//
// async fn write_to_senders(stream: OrdinalStreamResult, senders: Vec<(usize, Sender<OperatorStreamItem>)>) {
//
// }
//
// fn extract_current_markers(streams: &mut Vec<(usize, Vec<ChunkedStream>)>) -> Result<Option<Result<Vec<(usize, Vec<Marker>)>, GenerationMarker>>, DataFusionError> {
//
// }
