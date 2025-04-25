use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use async_trait::async_trait;
use datafusion::common::internal_datafusion_err;
use datafusion::error::DataFusionError;
use futures::Stream;
use futures::StreamExt;
use futures_util::stream::FuturesUnordered;
use futures_util::TryStreamExt;
use tokio::sync::Mutex;
use crate::streaming::operators::task_function::{CreateOperatorFunction2, OperatorFunction2, SItem};

#[derive(Clone)]
pub enum OperatorSpec {
    Dummy
}

impl CreateOperatorFunction2 for OperatorSpec {
    fn create_operator_function(&self) -> Box<dyn OperatorFunction2 + Sync + Send> {
        todo!()
    }
}

#[derive(Clone)]
pub struct OperatorInput {
    stream_id: String,
    ordinal: usize,
}

#[derive(Clone)]
pub struct OperatorOutput {
    stream_id: String,
    ordinal: usize,
}

#[derive(Clone)]
pub struct OperatorDefinition {
    pub id: String,
    pub state_id: String,
    pub spec: OperatorSpec,
    pub inputs: Vec<OperatorInput>,
    pub outputs: Vec<OperatorOutput>,
}

pub struct NestedOperator {
    inputs: Vec<(usize, String)>,
    operators: Vec<OperatorDefinition>,
    outputs: Vec<(usize, String)>,
}

impl CreateOperatorFunction2 for NestedOperator {
    fn create_operator_function(&self) -> Box<dyn OperatorFunction2 + Sync + Send> {
        Box::new(NestedOperatorFunction {
            input_stream_ids: self.inputs.clone(),
            operator_functions: self.operators.iter()
                .map(|op| (
                    op.clone(),
                    op.spec.create_operator_function()
                ))
                .collect(),
            output_stream_ids: self.outputs.clone(),
        })
    }
}

struct NestedOperatorFunction {
    input_stream_ids: Vec<(usize, String)>,
    operator_functions: Vec<(OperatorDefinition, Box<dyn OperatorFunction2 + Sync + Send>)>,
    output_stream_ids: Vec<(usize, String)>,
}

#[async_trait]
impl OperatorFunction2 for NestedOperatorFunction {
    // TODO add state id here
    async fn init(&mut self) {
        self.operator_functions.iter_mut()
            .map(|(_, op)| async move { op.init().await })
            .collect::<FuturesUnordered<_>>()
            // Ignore all elements
            .collect::<()>()
            .await;
    }

    async fn run<'a>(
        &'a mut self,
        inputs: Vec<(usize, Vec<Pin<Box<dyn Stream<Item=SItem> + Send + Sync>>>)>,
    ) -> Vec<(usize, Vec<Pin<Box<dyn Stream<Item=SItem> + Send + Sync>>>)> {
        let channels = Arc::new(IntraChannels::new());

        // Add all inputs to the channel map
        let input_stream_id_lookup = self.input_stream_ids.iter()
            .map(|(ordinal, stream_id)| (*ordinal, stream_id.clone()))
            .collect::<HashMap<_, _>>();
        for (ordinal, streams) in inputs.into_iter() {
            let stream_id = input_stream_id_lookup.get(&ordinal)
                .ok_or(internal_datafusion_err!("No stream id found for ordinal {}", ordinal)).unwrap();
            channels.add_streams(stream_id.clone(), streams).await.unwrap();
        }

        // Start all nested operators. If this operator contains any output operators, this future
        // will block until they are finished. For that reason, this type of nested operator cannot
        // support a mix of some output operators and some output streams. It must either contain
        // all output operators or all output streams.
        self.operator_functions.iter_mut()
            .map(|(operator_definition, operator_function)| {
                async {
                    run_operator(operator_definition, operator_function.as_mut(), &channels).await
                }
            })
            .collect::<FuturesUnordered<_>>()
            .try_collect::<()>()
            .await
            .unwrap();

        // Extract the output streams from the channel map
        let x = self.output_stream_ids.iter()
            .map(|(ordinal, stream_id)| {
                async {
                    (ordinal.clone(), channels.take_receivers(stream_id).await)
                }
            })
            .collect::<FuturesUnordered<_>>()
            .collect::<Vec<_>>()
            .await;
        x
    }

    async fn close(mut self: Box<Self>) {
        self.operator_functions.into_iter()
            .map(|(_, op)| async { op.close().await })
            .collect::<FuturesUnordered<_>>()
            // Ignore all elements
            .collect::<()>()
            .await;
    }
}

async fn run_operator<'a>(
    operator_definition: &OperatorDefinition,
    operator_function: &'a mut (dyn OperatorFunction2 + Sync + Send),
    channels: &'a IntraChannels,
) -> Result<(), DataFusionError> {
    // Get all inputs. This future will resolve once all upstream operators registered their inputs
    let input_streams = operator_definition.inputs.iter()
        .map(|input_spec| {
            async move {
                (input_spec.ordinal, channels.take_receivers(&input_spec.stream_id).await)
            }
        })
        .collect::<FuturesUnordered<_>>()
        .collect::<Vec<_>>()
        .await;

    let outputs = operator_function.run(input_streams).await;

    // Add the outputs to the channels
    let stream_id_lookup = operator_definition.outputs.iter()
        .map(|output_spec| (output_spec.ordinal, output_spec.stream_id.clone()))
        .collect::<HashMap<_, _>>();
    for (ordinal, streams) in outputs {
        let stream_id = stream_id_lookup.get(&ordinal)
            .ok_or(internal_datafusion_err!("No stream id found for ordinal {}", ordinal))?;
        let fut = channels.add_streams(stream_id.clone(), streams);
        let r = fut.await;
        r?;
    }

    // For all intermediate operators, the run function completes very quickly, as the streams just
    // need to be added to the channel map. However, the .run() method on output operators will not
    // complete until all streams are fully consumed.
    Ok(())
}

struct IntraChannels {
    channels: Mutex<HashMap<String, Vec<Pin<Box<dyn Stream<Item=SItem> + Send + Sync>>>>>
}

impl IntraChannels {
    fn new() -> Self {
        Self {
            channels: Mutex::new(HashMap::new())
        }
    }

    // TODO this should actually wait until the values appear
    async fn take_receivers(&self, id: &str) -> Vec<Pin<Box<dyn Stream<Item=SItem> + Send + Sync>>> where {
        let channels = &mut *self.channels.lock().await;
        channels.remove(id).unwrap()
    }

    async fn add_streams(&self, id: String, streams: Vec<Pin<Box<dyn Stream<Item=SItem> + Send + Sync>>>) -> Result<(), DataFusionError> {
        let channels = &mut *self.channels.lock().await;
        match channels.entry(id.clone()) {
            Entry::Vacant(slot) => {
                slot.insert(streams);
                Ok(())
            },
            Entry::Occupied(_) => {
                Err(internal_datafusion_err!("Stream id {} already exists", id.clone()))
            },
        }
    }
}
