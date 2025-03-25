use crate::streaming::action_stream::{ActionStreamAdapter, SendableActionStream, StreamItem, StreamResult};
use async_stream::__private::AsyncStream;
use async_stream::{stream, try_stream};
use datafusion::arrow::datatypes::SchemaRef;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use datafusion::common::internal_datafusion_err;
use datafusion::error::DataFusionError;
use futures::Stream;
use futures_util::StreamExt;
use tokio::sync::RwLock;
use tokio::time::sleep;
use crate::proto::generated::streaming_tasks as proto;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum OutputBufferSlotState {
    Running,
    Finished,
    Cancelled,
}

struct OutputPartition {
    pub items: Vec<StreamItem>,
    pub checkpoint_offsets: HashMap<u64, usize>,
}

struct OutputGeneration {
    pub generation_id: usize,
    pub starting_checkpoint: u64,
    pub ending_checkpoint: Option<u64>,
    // pub pre_computed_partitions: Vec<OutputPartition>,
    pub partition_range: Vec<usize>,
    pub unpartitioned_output: OutputPartition,
}

#[derive(Clone)]
enum OutputPartitioningStrategy {
    RoundRobin,
    Hash,
}

#[derive(Clone)]
pub struct OutputSlotPartitioning {
    strategy: OutputPartitioningStrategy,
    pub total_partitions: usize,
    pub partition_range: Vec<usize>,
}

impl TryInto<proto::OutputSlotPartitioning> for OutputSlotPartitioning {
    type Error = DataFusionError;

    fn try_into(self) -> Result<proto::OutputSlotPartitioning, Self::Error> {
        let strategy = match self.strategy {
            OutputPartitioningStrategy::RoundRobin => proto::output_partition_strategy::Strategy::RoundRobin(proto::RoundRobinPartitioning {}),
            OutputPartitioningStrategy::Hash => proto::output_partition_strategy::Strategy::Hash(proto::HashPartitioning {}),
        };

        Ok(proto::OutputSlotPartitioning {
            strategy: Some(proto::OutputPartitionStrategy{ strategy: Some(strategy) }),
            total_partitions: self.total_partitions as u64,
            partition_range: self.partition_range.iter().map(|v| *v as u64).collect(),
        })
    }
}

impl TryFrom<proto::OutputSlotPartitioning> for OutputSlotPartitioning {
    type Error = DataFusionError;

    fn try_from(proto: proto::OutputSlotPartitioning) -> Result<Self, Self::Error> {
        let proto_strategy = proto.strategy
            .ok_or(internal_datafusion_err!("OutputSlotPartitioning strategy is required"))?
            .strategy
            .ok_or(internal_datafusion_err!("OutputSlotPartitioning strategy is required"))?;
        let strategy = match proto_strategy {
            proto::output_partition_strategy::Strategy::RoundRobin(_) => OutputPartitioningStrategy::RoundRobin,
            proto::output_partition_strategy::Strategy::Hash(_) => OutputPartitioningStrategy::Hash,
        };

        Ok(Self {
            strategy,
            total_partitions: proto.total_partitions as usize,
            partition_range: proto.partition_range.iter().map(|v| *v as usize).collect(),
        })
    }
}

struct OutputBufferSlot {
    pub schema: SchemaRef,
    pub state: OutputBufferSlotState,
    pub partitioning: Option<OutputSlotPartitioning>,
    pub generations: Vec<OutputGeneration>,
}

pub struct OutputManager {
    slots: RwLock<HashMap<String, Arc<RwLock<OutputBufferSlot>>>>,
}

impl OutputManager {
    pub fn new() -> Self {
        Self {
            slots: RwLock::new(HashMap::new()),
        }
    }

    pub async fn create_slot(
        &self,
        output_stream_id: String,
        schema: SchemaRef,
        partitioning: Option<OutputSlotPartitioning>,
    ) {
        let slot = OutputBufferSlot {
            schema,
            state: OutputBufferSlotState::Running,
            partitioning,
            generations: Vec::new(),
        };
        let mut slots = self.slots.write().await;
        let existing = slots.insert(output_stream_id, Arc::new(RwLock::new(slot)));
        assert!(existing.is_none(), "Slot already exists");
    }

    pub async fn append_slot_generation(
        &self,
        output_stream_id: &str,
        generation_id: usize,
        partitions: Vec<usize>,
        starting_checkpoint: u64,
    ) {
        let generation = OutputGeneration {
            generation_id,
            starting_checkpoint,
            ending_checkpoint: None,
            partition_range: partitions,
            unpartitioned_output: OutputPartition {
                items: Vec::new(),
                checkpoint_offsets: [(starting_checkpoint, 0)].into_iter().collect(),
            },
            // pre_computed_partitions: partitions.into_iter()
            //     .map(|partition| OutputPartition {
            //         items: Vec::new(),
            //         checkpoint_offsets: [(starting_checkpoint, 0)].into_iter().collect(),
            //     })
            //     .collect(),
        };
        let slot = {
            let slots = self.slots.read().await;
            slots.get(output_stream_id).unwrap().clone()
        };
        slot.write().await.generations.push(generation);
    }

    pub async fn set_last_slot_ending_checkpoint(
        &self,
        output_stream_id: &str,
        generation_id: usize,
        ending_checkpoint: u64,
    ) {
        let slot = {
            let slots = self.slots.read().await;
            slots.get(output_stream_id).unwrap().clone()
        };
        let mut slot = slot.write().await;
        let generation = slot.generations.iter_mut().find(|g| g.generation_id == generation_id).unwrap();
        generation.ending_checkpoint = Some(ending_checkpoint);
    }

    pub async fn write(
        &self,
        output_stream_id: &str,
        generation_id: usize,
        value: StreamItem,
    ) -> Result<(), ()> {
        let slot = {
            let slots = self.slots.read().await;
            slots.get(output_stream_id).cloned()
        }.ok_or(())?;

        let slot = &mut *slot.write().await;
        let generation = slot.generations.iter_mut().find(|g| g.generation_id == generation_id).ok_or(())?;
        // assert!(partition < generation.pre_computed_partitions.len());

        // let partition = &mut generation.pre_computed_partitions[partition];
        match value {
            StreamItem::RecordBatch(batch) => {
                generation.unpartitioned_output.items.push(StreamItem::RecordBatch(batch));
            },
            StreamItem::Marker(marker) => {
                let checkpoint_number = marker.checkpoint_number;
                generation.unpartitioned_output.items.push(StreamItem::Marker(marker));
                generation.unpartitioned_output.checkpoint_offsets.insert(checkpoint_number, generation.unpartitioned_output.items.len());
            }
        }

        Ok(())
    }

    pub async fn finish(&self, output_stream_id: &str) -> Result<(), ()> {
        let slot = {
            let slots = self.slots.read().await;
            slots.get(output_stream_id).cloned()
        }.ok_or(())?;

        let slot = &mut *slot.write().await;
        slot.state = OutputBufferSlotState::Finished;

        Ok(())
    }

    pub async fn stream_output(
        &self,
        output_stream_id: &str,
        partition: usize,
        starting_checkpoint: Option<u64>,
    ) -> Result<SendableActionStream, ()> {
        // TODO throw an error if partition isn't found or disappears in a generation
        let slot = {
            let slots = &*self.slots.read().await;
            let slot = slots.get(output_stream_id)
                .ok_or(())?;
            Arc::clone(slot)
        };

        let schema: SchemaRef = {
            SchemaRef::clone(&slot.read().await.schema)
        };

        let stream: AsyncStream<StreamResult, _> = try_stream! {
            // Start the stream by skipping records until we reach the desired starting checkpoint
            let (mut generation_index, index) = if let Some(starting_checkpoint) = starting_checkpoint {
                Self::find_starting_point(&slot, starting_checkpoint).await
                    .map_err(|_| DataFusionError::Internal("Failed to find starting point".to_string()))?
            } else {
                (0, 0)
            };

            println!("found stream starting point: generation_index={}, index={}", generation_index, index);

            loop {
                // Check if the entire stream is finished
                {
                    let slot = &*slot.read().await;
                    if slot.state == OutputBufferSlotState::Finished && generation_index >= slot.generations.len() {
                        break;
                    }
                }

                // Pull all the items from the current generation. Stream needs to be pinned because
                // it is used across an await boundary
                let mut stream = Box::pin(Self::stream_generation(slot.clone(), partition, generation_index, index));
                while let Some(next) = stream.next().await {
                    yield next
                }

                generation_index += 1;
            }
        };

        Ok(Box::pin(ActionStreamAdapter::new(schema, stream)))
    }

    async fn find_starting_point(slot: &RwLock<OutputBufferSlot>, starting_checkpoint: u64) -> Result<(usize, usize), ()> {
        let mut current_generation_index = 0;
        loop {
            {
                let slot = &*slot.read().await;
                if slot.state == OutputBufferSlotState::Finished {
                    return Err(());
                }

                for (generation_index, generation) in slot.generations.iter().enumerate().skip(current_generation_index) {
                    if generation.starting_checkpoint > starting_checkpoint {
                        panic!("Could not find a point to stream from");
                    }
                    if generation.starting_checkpoint == starting_checkpoint {
                        return Ok((generation_index, 0));
                    }

                    // Check if this generation finished before our starting point
                    if let Some(end) = generation.ending_checkpoint {
                        if end < starting_checkpoint {
                            continue;
                        }
                    }

                    // Check inside this generation to see if we can find the starting checkpoint
                    let partition = &generation.unpartitioned_output;
                    if let Some(offset) = partition.checkpoint_offsets.get(&starting_checkpoint) {
                        return Ok((generation_index, *offset));
                    }

                    // If we reached this point, this generation could contain our starting point, but
                    // it might not be ready yet. So we are going to sleep for a little while, and then
                    // look again
                    current_generation_index = generation_index;
                    break;
                }
            } // Make sure we release the lock before sleeping

            sleep(Duration::from_millis(100)).await;
        }
    }

    fn stream_generation(
        slot: Arc<RwLock<OutputBufferSlot>>,
        partition: usize,
        generation_index: usize,
        starting_index: usize,
    ) -> impl Stream<Item=StreamItem> {
        stream! {
            let mut index = starting_index;
            loop {
                // Read the next item from the generation
                let (result, finished) = {
                    // It's important that we don't hold the lock for a long time
                    let slot = &*slot.read().await;
                    let finished = slot.state == OutputBufferSlotState::Finished;

                    match slot.generations.get(generation_index) {
                        None => (IterateGenerationHelper::GenerationDoesntExist, finished),
                        Some(generation) => {
                            let partition = &slot.generations[generation_index].unpartitioned_output;
                            // TODO filter partition
                            match partition.items.get(index).cloned() {
                                Some(item) => (IterateGenerationHelper::ItemFound(item), finished),
                                None if generation.ending_checkpoint.is_none() => {
                                    (IterateGenerationHelper::ItemNotReady, finished)
                                },
                                _ => {
                                    (IterateGenerationHelper::GenerationFinished, finished)
                                },
                            }
                        },
                    }
                };

                match (result, finished) {
                    (IterateGenerationHelper::ItemFound(item), _) => {
                        index += 1;
                        if let StreamItem::Marker(marker) = &item {
                            println!("Stream generation found marker: {}", marker.checkpoint_number);
                        }
                        yield item;
                    },
                    (IterateGenerationHelper::ItemNotReady, _) => {
                        // The item is not ready yet, so we wait a while and try again
                        sleep(Duration::from_millis(50)).await;
                        continue;
                    },
                    (IterateGenerationHelper::GenerationDoesntExist, false) => {
                        // The generation didn't exist yet, but the stream is not complete, so we
                        // wait a while and try again
                        sleep(Duration::from_millis(50)).await;
                        continue;
                    },
                    (IterateGenerationHelper::GenerationDoesntExist, true) => {
                        // We reached the end of the stream
                        break;
                    },
                    (IterateGenerationHelper::GenerationFinished, _) => {
                        break;
                    },
                }
            }
        }
    }
}

enum IterateGenerationHelper {
    GenerationDoesntExist,
    ItemFound(StreamItem),
    ItemNotReady,
    GenerationFinished,
}
