use crate::streaming::action_stream::Marker;
use crate::streaming::generation::{GenerationInputDetail, GenerationSpec, GenerationStartOffset};
use crate::streaming::operators::task_function::{CreateOperatorFunction2, OperatorFunction2, SItem};
use crate::streaming::operators::utils::borrowed_stream::BorrowedStream;
use crate::streaming::operators::utils::fiber_stream::FiberStream;
use crate::streaming::runtime::Runtime;
use async_trait::async_trait;
use datafusion::common::internal_datafusion_err;
use datafusion::error::DataFusionError;
use futures::stream::iter;
use futures::{Stream, StreamExt};
use futures_util::stream::FuturesUnordered;
use futures_util::{FutureExt, TryStreamExt};
use std::collections::{HashMap, HashSet};
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::sync::Arc;
use eyeball::{AsyncLock, SharedObservable};
use futures_util::future::Shared;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tokio::sync::oneshot::{Receiver, Sender};
use crate::streaming::utils::create_remote_stream;

#[derive(Clone, Serialize, Deserialize)]
pub struct RemoteSourceOperator {
    stream_ids: Vec<String>,
}

impl RemoteSourceOperator {
    pub fn new(stream_ids: Vec<String>) -> Self {
        Self { stream_ids }
    }

    pub fn get_stream_ids(&self) -> &[String] {
        &self.stream_ids
    }
}

impl CreateOperatorFunction2 for RemoteSourceOperator {
    fn create_operator_function(&self) -> Box<dyn OperatorFunction2 + Sync + Send> {
        Box::new(RemoteSourceOperatorFunction::new(self.stream_ids.clone()))
    }
}

struct RemoteSourceOperatorFunction {
    stream_ids: Vec<String>,
    runtime: Option<Arc<Runtime>>,
    scheduling_details_state: Option<Arc<Mutex<(Vec<GenerationSpec>, Vec<GenerationInputDetail>)>>>,
    loaded_checkpoint: usize,
}

impl RemoteSourceOperatorFunction {
    fn new(stream_ids: Vec<String>) -> Self {
        Self {
            stream_ids,
            runtime: None,
            scheduling_details_state: None,
            loaded_checkpoint: 0,
        }
    }
}

#[async_trait]
impl OperatorFunction2 for RemoteSourceOperatorFunction {
    async fn init(
        &mut self,
        runtime: Arc<Runtime>,
        scheduling_details: SharedObservable<(Option<Vec<GenerationSpec>>, Option<Vec<GenerationInputDetail>>), AsyncLock>
    ) -> Result<(), DataFusionError> {
        self.runtime = Some(runtime);
        self.scheduling_details_state = Some(Arc::new(Mutex::new((Vec::new(), Vec::new())))); // Initialize with empty details
        // TODO We'll populate scheduling details in actual implementation
        Ok(())
    }

    async fn load(&mut self, checkpoint: usize) -> Result<(), DataFusionError> {
        self.loaded_checkpoint = checkpoint;
        Ok(())
    }

    // Since this is an input operator, it takes no inputs, instead just reading from the remote
    // sources.
    // TODO change the output to FiberStream concept
    async fn run<'a>(
        &'a mut self,
        inputs: Vec<(usize, Box<dyn FiberStream<Item=Result<SItem, DataFusionError>> + Send + Sync + 'a>)>,
    ) -> Result<Vec<(usize, Box<dyn FiberStream<Item=Result<SItem, DataFusionError>> + Send + Sync + 'a>)>, DataFusionError> {
        assert_eq!(inputs.len(), 0);

        let scheduling_details_state = self.scheduling_details_state.as_ref().ok_or_else(|| {
            internal_datafusion_err!("Scheduling details state not initialized. Did you call init()?")
        })?;
        let runtime = self.runtime.as_ref().ok_or_else(|| {
            internal_datafusion_err!("Runtime not initialized. Did you call init()?")
        })?;

        // Create RunningStream directly in the run method
        let running_stream = RunningStream::new(
            runtime.clone(),
            self.stream_ids.clone(),
            scheduling_details_state.clone(),
            self.loaded_checkpoint,
        );

        Ok(vec![(0, Box::new(running_stream) as Box<dyn FiberStream<Item=Result<SItem, DataFusionError>> + Send + Sync>)])
    }

    async fn last_checkpoint(&self) -> usize {
        0
    }

    async fn close(self: Box<Self>) {
        // No-op
    }
}



struct RunningStream {
    runtime: Arc<Runtime>,
    stream_ids: Vec<String>,
    scheduling_details_state: Arc<Mutex<(Vec<GenerationSpec>, Vec<GenerationInputDetail>)>>,
    current_marker: usize,
    existing_fibers: Option<HashMap<(String, String), RunningFiber>>,
    current_generation: Option<GenerationSpec>,
}

impl RunningStream {
    pub fn new(
        runtime: Arc<Runtime>,
        stream_ids: Vec<String>,
        scheduling_details_state: Arc<Mutex<(Vec<GenerationSpec>, Vec<GenerationInputDetail>)>>,
        current_marker: usize,
    ) -> Self {
        Self {
            runtime,
            stream_ids,
            scheduling_details_state,
            current_marker,
            existing_fibers: None,
            current_generation: None,
        }
    }

    fn make_fibers_for_all_addresses<'a>(
        runtime: &'a Arc<Runtime>,
        stored_existing_fibers: &'a mut Option<HashMap<(String, String), RunningFiber>>,
        addresses: Vec<(String, String, Vec<usize>)>,
    ) -> &'a mut HashMap<(String, String), RunningFiber> {
        let (mut existing_fibers, new_addresses) = match stored_existing_fibers.take() {
            // Reuse the existing fibres where possible
            Some(existing_fibers) => intersect_addresses_and_fibers(
                &addresses,
                existing_fibers,
            ),
            None => (Default::default(), addresses),
        };

        // Create new fibres for the new addresses
        for (stream_id, address, partitions) in new_addresses {
            let fiber = RunningFiber::new(runtime.clone(), stream_id.clone(), address);
            existing_fibers.push((stream_id, fiber, partitions));
        }

        // Update the current partitions for each fibre
        for (_stream_id, fiber, partitions) in &mut existing_fibers {
            fiber.load(partitions.clone());
        }

        // Insert the new set of fibres back into the map
        stored_existing_fibers.insert(existing_fibers.into_iter()
            .map(|(stream_id, fiber, _partitions)| ((stream_id, fiber.address.clone()), fiber))
            .collect())
    }
}

#[async_trait]
impl FiberStream for RunningStream {
    type Item = Result<SItem, DataFusionError>;

    async fn pull<'a>(&'a mut self) -> Result<Vec<Box<dyn Stream<Item=Result<SItem, DataFusionError>> + Sync + Send + 'a>>, DataFusionError> {
        let (generations, input_details) = {
            let guard = self.scheduling_details_state.lock().await;
            guard.deref().clone()
        };

        let current_marker = self.current_marker;

        // Lookup current generation
        // Get starting partitions
        let current_generation = find_current_generation(&self.stream_ids, &generations, current_marker)?;
        let current_partitions = &current_generation.partitions;

        // Get current input details
        // Find all addresses that cover the current partitions at our current checkpoint
        // This assumes that only relevant input details are passed to the operator
        let addresses = get_addresses(current_partitions, current_marker, input_details.as_slice())?;

        // Create streams for all addresses
        let existing_fibers = Self::make_fibers_for_all_addresses(
            &self.runtime,
            &mut self.existing_fibers,
            addresses.clone(),
        );
        let shared_state = Arc::new(SharedFiberState::new(
            &mut self.current_marker,
            &self.stream_ids,
            &self.scheduling_details_state,
            current_generation.id.clone(),
            current_partitions.clone(),
            addresses,
        ));
        let streams = existing_fibers.values_mut()
            .map(|fiber| {
                async {
                    run_fiber_with_lifetimes(fiber, shared_state.clone()).await
                }
            })
            .collect::<FuturesUnordered<_>>()
            .try_collect::<Vec<_>>()
            .await?;

        Ok(streams)


        // Return the fibre stream containing a stream from each address
        // Output: Box<dyn FiberStream<Item=Result<SItem, DataFusionError>> + Sync + Send>
        // Inside a single fibre stream, we will pause at each marker, to check if the number of fibres
        // is still the same, and to check if we need to move to the next generation.
        // Generation markers do not travel between tasks, only between operators within a task.
    }

}

async fn run_fiber_with_lifetimes<'a>(
    fiber: &'a mut RunningFiber,
    shared_state: Arc<SharedFiberState<'a>>,
) -> Result<Box<dyn Stream<Item=Result<SItem, DataFusionError>> + Sync + Send + 'a>, DataFusionError> {
    fiber.run(shared_state.clone()).await
}

fn find_current_generation<'a>(
    stream_ids: &[String],
    generations: &'a [GenerationSpec],
    current_marker: usize,
) -> Result<&'a GenerationSpec, DataFusionError> {
    // Find the latest generation that has a start condition that is valid for the current marker
    let stream_ids_set = stream_ids.iter().cloned().collect::<HashSet<_>>();
    for generation in generations.iter().rev() {
        let valid = generation.start_conditions.iter()
            .filter(|(condition_stream_id, _start_condition)| stream_ids_set.contains(condition_stream_id))
            .all(|(_condition_stream_id, start_condition)| {
                match start_condition {
                    GenerationStartOffset::AnyTimeAfter(minimum_checkpoint) => {
                        minimum_checkpoint <= &current_marker
                    },
                }
            });
        if valid {
            return Ok(generation);
        }
    }
    Err(DataFusionError::Execution("No generation found for the current marker".to_string()))
}

fn get_addresses<'a>(
    partitions: &[usize],
    checkpoint: usize,
    input_details: &[GenerationInputDetail],
) -> Result<Vec<(String, String, Vec<usize>)>, DataFusionError> { // returns (stream_id, address, partitions)
    let partitions_set: HashSet<_> = partitions.iter().copied().collect();
    input_details.iter()
        .map(|input_detail| get_addresses_for_stream(&partitions_set, checkpoint, input_detail))
        .collect::<Result<Vec<_>, DataFusionError>>()
        // Flatten
        .map(|addresses| addresses.into_iter().flatten().collect::<Vec<_>>())
}

fn get_addresses_for_stream(
    partitions: &HashSet<usize>,
    checkpoint: usize,
    input_details: &GenerationInputDetail,
) -> Result<Vec<(String, String, Vec<usize>)>, DataFusionError> { // returns (stream_id, address, partitions)
    let mut all_found_partitions = HashSet::new();
    let addresses = input_details.locations.iter()
        // Check that the location range includes the checkpoint
        .filter(|location| location.offset_range.0 <= checkpoint && location.offset_range.1 > checkpoint)
        // Check that the partitions overlap
        .filter_map(|location| {
            let location_partitions_set = location.partitions.iter().copied().collect();
            let overlapping_partitions = partitions.intersection(&location_partitions_set)
                .copied()
                .collect::<Vec<_>>();
            if overlapping_partitions.is_empty() {
                None
            } else {
                all_found_partitions.extend(overlapping_partitions.iter().copied());
                Some((input_details.stream_id.clone(), location.address.clone(), overlapping_partitions))
            }
        })
        .collect::<Vec<_>>();

    // Check that we actually found all the partitions
    let missing_partitions = partitions.iter()
        .filter(|partition| !all_found_partitions.contains(partition))
        .collect::<Vec<_>>();
    if !missing_partitions.is_empty() {
        return Err(DataFusionError::Execution(format!(
            "Missing partitions {:?} for stream {} at checkpoint {}",
            missing_partitions, input_details.stream_id, checkpoint
        )));
    }

    Ok(addresses)
}

fn intersect_addresses_and_fibers(
    addresses: &[(String, String, Vec<usize>)], // (stream_id, address, partitions)
    mut fibers: HashMap<(String, String), RunningFiber>,
) -> (Vec<(String, RunningFiber, Vec<usize>)>, Vec<(String, String, Vec<usize>)>) {
    let mut existing_fibers = Vec::new();
    let mut new_addresses = Vec::new();
    for (stream_id, address, partitions) in addresses {
        match fibers.remove(&(stream_id.clone(), address.clone())) {
            None => new_addresses.push((stream_id.clone(), address.clone(), partitions.clone())),
            Some(fiber) => existing_fibers.push((stream_id.clone(), fiber, partitions.clone())),
        };
    }

    (existing_fibers, new_addresses)
}


#[derive(Clone)]
enum FiberState {
    Continue,
    Pause,
}

struct SharedFiberState<'a> {
    stream_ids: &'a [String],
    scheduling_details_state: &'a Mutex<(Vec<GenerationSpec>, Vec<GenerationInputDetail>)>,
    current_generation_id: String,
    current_partitions: Vec<usize>,
    current_addresses: Vec<(String, String, Vec<usize>)>,
    shared_trigger_state: Mutex<(
        &'a mut usize, // Last completed marker
        Vec<Result<Sender<FiberState>, Shared<Receiver<FiberState>>>>,
    )>
}

impl <'a> SharedFiberState<'a> {
    pub fn new(
        last_completed_marker: &'a mut usize,
        stream_ids: &'a [String],
        scheduling_details_state: &'a Mutex<(Vec<GenerationSpec>, Vec<GenerationInputDetail>)>,
        current_generation_id: String,
        current_partitions: Vec<usize>,
        current_addresses: Vec<(String, String, Vec<usize>)>,
    ) -> Self {
        let shared_trigger_state = Mutex::new((
            last_completed_marker,
            Self::create_shared_trigger_state(current_addresses.len()),
        ));
        Self {
            stream_ids,
            scheduling_details_state,
            current_generation_id,
            current_partitions,
            current_addresses,
            shared_trigger_state,
        }
    }

    pub async fn poll(&self, marker: usize) -> Result<FiberState, DataFusionError> {
        let result = {
            let mut guard = self.shared_trigger_state.lock().await;
            let (last_completed_marker, state) = guard.deref_mut();

            // Check if we are the last fibre to reach the marker
            match state.pop() {
                None => Err(internal_datafusion_err!("No shared trigger state available")),
                Some(Err(shared_future)) => {
                    // We are not the last fibre, so we need to wait for the shared future after
                    // releasing the lock
                    Ok(Err(shared_future))
                },
                Some(Ok(sender)) => {
                    // We are the last fibre, so we need to check the generation and addresses, then
                    // reset the shared state all while maintaining the lock.

                    // Update the last completed marker
                    **last_completed_marker = marker;

                    let result = self.check_generation_and_addresses(marker, state.as_mut()).await?;
                    sender.send(result.clone())
                        .map_err(|_| DataFusionError::Execution("Failed to send continue signal".to_string()))?;
                    Ok(Ok(result))
                }
            }
        }?;

        match result {
            Ok(next_fiber_state) => Ok(next_fiber_state),
            Err(receiver) => receiver.await
                .map_err(|_| DataFusionError::Execution("Failed to receive continue signal".to_string()))
        }
    }

    async fn check_generation_and_addresses(
        &self, marker: usize,
        shared_trigger_state: &mut Vec<Result<Sender<FiberState>, Shared<Receiver<FiberState>>>>,
    ) -> Result<FiberState, DataFusionError> {
        {
            let details = self.scheduling_details_state.lock().await;

            // Look up the generation for this marker
            let current_generation = find_current_generation(
                self.stream_ids,
                details.0.as_slice(),
                marker,
            )?.clone();

            // If we have moved to the next generation, we need to stop streaming
            if current_generation.id != self.current_generation_id {
                return Ok(FiberState::Pause);
            }

            // Check that all the fibres are still valid
            let new_addresses = get_addresses(
                &current_generation.partitions,
                marker,
                details.1.as_slice(),
            )?;
            if new_addresses.iter().collect::<HashSet<_>>() != self.current_addresses.iter().collect::<HashSet<_>>() {
                // Addresses have changed
                return Ok(FiberState::Pause);
            }
        };

        // All fibres are allowed to continue streaming without interruption
        // Set up the next round of the shared trigger state
        *shared_trigger_state = Self::create_shared_trigger_state(self.current_addresses.len());
        Ok(FiberState::Continue)
    }

    fn create_shared_trigger_state(address_count: usize) -> Vec<Result<Sender<FiberState>, Shared<Receiver<FiberState>>>> {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        std::iter::once(Ok(sender))
            .chain(std::iter::repeat_n(receiver.shared(), address_count - 1).map(Err))
            .collect()
    }
}


enum RunningFiberCheckMarker<T> {
    Item(T),
    Check(Marker),
}

struct RunningFiber {
    runtime: Arc<Runtime>,
    stream_id: String,
    address: String,
    current_partitions: Option<Vec<usize>>,
    // TODO generation markers shouldn't be here.
    remote_stream: Option<Pin<Box<dyn Stream<Item=Result<SItem, DataFusionError>> + Send + Sync>>>,
}

impl RunningFiber {
    pub fn new(runtime: Arc<Runtime>, stream_id: String, address: String) -> Self {
        Self {
            runtime,
            stream_id,
            address,
            current_partitions: None,
            remote_stream: None,
        }
    }

    pub fn load(&mut self, partitions: Vec<usize>) {
        match &self.current_partitions {
            None => {
                self.current_partitions = Some(partitions);
            }
            Some(current_partitions) => {
                if current_partitions != &partitions {
                    // Clear the remote stream since we need to recreate it
                    self.remote_stream = None;
                    self.current_partitions = Some(partitions);
                }
            }
        }
    }

    pub async fn run<'a, 'b>(
        &'a mut self,
        shared_state: Arc<SharedFiberState<'b>>,
    ) -> Result<Box<dyn Stream<Item=Result<SItem, DataFusionError>> + Send + Sync + 'b>, DataFusionError>
    where 'a: 'b
    {
        // TODO don't panic here
        let current_partitions = self.current_partitions.clone().unwrap();
        let remote_stream = if self.remote_stream.is_none() {
            let stream = create_remote_stream::create_remote_stream(
                &self.runtime,
                &self.stream_id,
                &self.address,
                current_partitions,
            ).await?;
            self.remote_stream.get_or_insert(Box::into_pin(stream))
        } else {
            // unwrap is safe here because we check if remote_stream is None above
            self.remote_stream.as_mut().unwrap()
        };

        // TODO also watch for abrupt end of this address, and throw an error if we pass it by
        //  accident
        let synced_stream = BorrowedStream::new(remote_stream)
            // Each time we reach a marker, check if we are allowed to continue
            .flat_map(|item| {
                match item {
                    // This double wraps the error
                    Ok(SItem::Marker(marker)) => iter(vec![
                        RunningFiberCheckMarker::Item(Ok(SItem::Marker(marker.clone()))),
                        RunningFiberCheckMarker::Check(marker),
                    ]),
                    other => iter(vec![RunningFiberCheckMarker::Item(other)])
                }
            })
            // We need to wrap the value in another result because we want to use try_take_while
            // to return an error, but we want to keep the original result value as input.
            // Yes, this is quite messy.
            .map(|value| -> Result<_, DataFusionError> { Ok(value) })
            .try_take_while(move |maybe_check| {
                let maybe_check_marker = match maybe_check {
                    RunningFiberCheckMarker::Item(_) => None,
                    RunningFiberCheckMarker::Check(marker) => Some(marker.clone())
                };
                let shared_state = shared_state.clone();
                async move {
                    match maybe_check_marker {
                        // If it's not a check, we just return true to continue
                        None => Ok(true),
                        Some(marker) => {
                            // Stop to check the shared state
                            shared_state.poll(marker.checkpoint_number as usize).await
                                .map(|fiber_state| matches!(fiber_state, FiberState::Continue))
                        },
                    }
                }
            })
            .flat_map(|result| {
                match result {
                    // Ignore the check markers
                    Ok(RunningFiberCheckMarker::Item(inner_result)) => iter(vec![inner_result]),
                    Ok(RunningFiberCheckMarker::Check(_)) => iter(vec![]),
                    Err(err) => iter(vec![Err(err)]),
                }
            });
        Ok(Box::new(synced_stream))
    }
}
