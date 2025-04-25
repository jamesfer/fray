use crate::streaming::action_stream::{Marker, MergedActionStream, OrdinalStreamItem, StreamItem};
use crate::streaming::checkpoint_storage_manager::CheckpointStorageManager;
use crate::streaming::input_manager::InputManager;
use crate::streaming::output_manager::{OutputManager, OutputSlotPartitioning};
use crate::streaming::task_definition::{TaskInputDefinition, TaskInputStreamGeneration};
use crate::streaming::operators::task_function::{OutputChannel, OutputChannelL, TaskFunction, TaskState};
use datafusion::arrow::datatypes::SchemaRef;
use futures_util::StreamExt;
use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

enum TaskType {
    Source,
    Intermediate,
}

pub struct TaskRunner {
    task_id: String,
    task_type: TaskType,
    task_inputs: Arc<Vec<TaskInputPhaseState>>,
    handle: JoinHandle<()>
}

impl TaskRunner {
    pub fn start(
        task_id: String,
        task_function: Box<dyn TaskFunction + Send + Sync>,
        task_inputs: TaskInputDefinition,
        output_stream_id: String,
        output_schema: SchemaRef,
        output_partitioning: Option<OutputSlotPartitioning>,
        checkpoint_id: String,
        task_checkpoint: Option<u64>,
        input_manager: Arc<InputManager>,
        output_manager: Arc<OutputManager>,
        checkpoint_storage_manager: Arc<CheckpointStorageManager>,
    ) -> Self {
        assert!(task_inputs.phases.len() > 0);
        assert!(task_inputs.phases[0].generations.len() > 0);
        let task_type = if task_inputs.phases[0].generations[0].streams.is_empty() {
            TaskType::Source
        } else {
            TaskType::Intermediate
        };

        let task_input_state = Arc::new(task_inputs.phases.into_iter()
            .map(|phase| TaskInputPhaseState {
                generations: Mutex::new(phase.generations)
            })
            .collect());
        let handle = {
            let task_id = task_id.clone();
            let task_input_state = Arc::clone(&task_input_state);
            tokio::spawn(async move {
                Self::run_task(
                    &task_id,
                    task_function,
                    task_input_state,
                    &output_stream_id,
                    output_schema,
                    output_partitioning,
                    &checkpoint_id,
                    task_checkpoint,
                    input_manager,
                    output_manager,
                    checkpoint_storage_manager,
                ).await;
            })
        };

        Self {
            task_id,
            task_type,
            handle,
            task_inputs: task_input_state,
        }
    }

    // pub async fn update_inputs(&self, new_inputs: Vec<TaskInputStreamGeneration>) {
    //     let mut task_inputs = self.task_inputs.lock().await;
    //     *task_inputs = new_inputs;
    // }

    async fn run_task(
        task_id: &str,
        mut task_function: Box<dyn TaskFunction + Send + Sync>,
        task_input_phases: Arc<Vec<TaskInputPhaseState>>,
        output_stream_id: &str,
        output_schema: SchemaRef,
        output_partitioning: Option<OutputSlotPartitioning>,
        checkpoint_id: &str,
        starting_point: Option<u64>,
        input_manager: Arc<InputManager>,
        output_manager: Arc<OutputManager>,
        checkpoint_storage_manager: Arc<CheckpointStorageManager>,
    ) {
        println!("Starting task {}", task_id);

        // Create a slot in the output for this task
        // TODO extract this to a higher level
        output_manager.create_slot(
            output_stream_id.to_string(),
            output_schema,
            output_partitioning.clone(),
        ).await;

        // Initialize the task
        task_function.init().await;

        // Outer loop iterates over each input phase
        let mut checkpoint_marker = starting_point.map(|checkpoint_number| Marker { checkpoint_number });
        let mut output_generation_id = 0;
        for phase in task_input_phases.iter() {
            let (next_checkpoint, next_output_generation_id) = Self::run_phase(
                task_id,
                task_function.as_mut(),
                checkpoint_id,
                checkpoint_marker,
                &phase.generations,
                output_stream_id,
                output_generation_id,
                output_partitioning.clone(),
                &input_manager,
                output_manager.clone(),
                &checkpoint_storage_manager
            ).await;
            checkpoint_marker = Some(next_checkpoint);
            output_generation_id = next_output_generation_id;
        }

    }

    async fn run_phase(
        task_id: &str,
        task_function: &mut (dyn TaskFunction + Send + Sync),
        checkpoint_id: &str,
        checkpoint_marker: Option<Marker>,
        input_generations: &Mutex<Vec<TaskInputStreamGeneration>>,
        output_stream_id: &str,
        mut output_generation_id: usize,
        output_partitioning: Option<OutputSlotPartitioning>,
        input_manager: &InputManager,
        output_manager: Arc<OutputManager>,
        checkpoint_storage_manager: &CheckpointStorageManager
    ) -> (Marker, usize) {
        let mut input_generation_index = 0;

        // Get the current input stream generations
        let mut current_input_generation = {
            let input_generations = &*input_generations.lock().await;
            input_generations.get(input_generation_index).unwrap().clone()
        };

        // Load the checkpoint for the new partitions if needed
        if let Some(checkpoint_marker) = &checkpoint_marker {
            let checkpoint_data = checkpoint_storage_manager.get_checkpoint_state(
                checkpoint_id.to_string(),
                current_input_generation.partition_range.as_slice(),
                checkpoint_marker.checkpoint_number,
            ).await.expect(format!("Failed to load checkpoint data. Task id: {}, partition range {:?}, checkpoint number, {}", task_id, current_input_generation.partition_range.as_slice(), checkpoint_marker.checkpoint_number).as_str());
            task_function.load_state(checkpoint_data).await;
        }

        let output_partitions = match &output_partitioning {
            Some(partitioning) => partitioning.partition_range.clone(),
            None => vec![0],
        };
        let mut output_sink = Self::create_new_output_slot_generation(
            output_manager.clone(),
            output_stream_id,
            output_generation_id,
            output_partitions,
            checkpoint_marker.unwrap_or_else(|| Marker { checkpoint_number: 0 }),
        ).await;

        let mut action_stream = Self::create_inputs_stream(
            &current_input_generation,
            input_manager,
        ).await;

        loop {
            // Consume the input until we need to move to the next input generation
            let result = Self::run_to_end_of_generation(
                task_id,
                input_generation_index,
                input_generations,
                task_function,
                &mut action_stream,
                current_input_generation.partition_range.as_slice(),
                checkpoint_id,
                &mut output_sink,
                checkpoint_storage_manager,
            ).await;

            println!("Task {} finished generation", task_id);

            match result {
                // Stream finished
                Err(last_marker) => {
                    println!("Task {} finished completely", task_id);
                    let last_marker = last_marker.expect(format!("Task {} finished without producing a final marker", task_id).as_str());
                    let last_checkpoint_number = last_marker.checkpoint_number;
                    output_manager.write(output_stream_id, output_generation_id, StreamItem::Marker(last_marker.clone())).await.unwrap();
                    output_manager.set_last_slot_ending_checkpoint(output_stream_id, output_generation_id, last_checkpoint_number).await;
                    output_manager.finish(output_stream_id).await.unwrap();
                    return (last_marker, output_generation_id);
                },
                // Prepare next generation
                Ok((next_inputs, next_index, marker)) => {
                    (action_stream, output_generation_id, output_sink) = Self::setup_next_generation(
                        task_function,
                        current_input_generation,
                        &next_inputs,
                        output_stream_id,
                        output_generation_id,
                        marker,
                        output_sink,
                        checkpoint_id,
                        &input_manager,
                        &output_manager,
                        checkpoint_storage_manager
                    ).await;
                    current_input_generation = next_inputs;
                    input_generation_index = next_index;
                }
            }
        }
    }

    async fn setup_initial_generation(
        task_id: String,
        task_function: &mut (dyn TaskFunction + Send + Sync),
        output_stream_id: &str,
        output_partitioning: Option<OutputSlotPartitioning>,
        inputs: &TaskInputStreamGeneration,
        checkpoint_id: &str,
        checkpoint_marker: Option<Marker>,
        input_manager: &InputManager,
        output_manager: &Arc<OutputManager>,
        checkpoint_storage_manager: &CheckpointStorageManager,
    ) -> (Option<MergedActionStream>, usize, OutputChannel) {
        let input_partition_range = &inputs.partition_range;

        // Load the checkpoint for the new partitions if needed
        if let Some(checkpoint_marker) = &checkpoint_marker {
            let checkpoint_data = checkpoint_storage_manager.get_checkpoint_state(
                checkpoint_id.to_string(),
                input_partition_range,
                checkpoint_marker.checkpoint_number,
            ).await.expect(format!("Failed to load checkpoint data. Task id: {}, partition range {:?}, checkpoint number, {}", task_id, input_partition_range, checkpoint_marker.checkpoint_number).as_str());
            task_function.load_state(checkpoint_data).await;
        }

        // Create output sink generation
        let output_generation_id = 0;
        let output_partitions = match &output_partitioning {
            Some(partitioning) => partitioning.partition_range.clone(),
            None => vec![0],
        };
        let output_sink = Self::create_new_output_slot_generation(
            output_manager.clone(),
            output_stream_id,
            output_generation_id,
            output_partitions,
            checkpoint_marker.unwrap_or_else(|| Marker { checkpoint_number: 0 }),
        ).await;

        let action_stream = Self::create_inputs_stream(
            inputs,
            input_manager,
        ).await;

        (action_stream, output_generation_id, output_sink)
    }

    async fn setup_next_generation(
        task_function: &mut (dyn TaskFunction + Send + Sync),
        previous_inputs: TaskInputStreamGeneration,
        next_inputs: &TaskInputStreamGeneration,
        output_stream_id: &str,
        mut output_generation_id: usize,
        current_checkpoint: Marker,
        mut output_sink: OutputChannel,
        checkpoint_id: &str,
        input_manager: &InputManager,
        output_manager: &Arc<OutputManager>,
        checkpoint_storage_manager: &CheckpointStorageManager,
    ) -> (Option<MergedActionStream>, usize, OutputChannel) {
        // If the partitions have changed, we need to create a new output generation
        let previous_partitions: HashSet<_> = previous_inputs.partition_range.iter().copied().collect();
        let next_partitions: HashSet<_> = next_inputs.partition_range.iter().copied().collect();
        if previous_partitions != next_partitions {
            let next_partitions_vec: Vec<_> = next_partitions.iter().copied().collect();

            // Load the checkpoint for the new partitions if needed
            let new_partitions = next_partitions.difference(&previous_partitions);
            if new_partitions.take(1).last().is_some() {
                let checkpoint_data = checkpoint_storage_manager.get_checkpoint_state(
                    checkpoint_id.to_string(),
                    next_partitions_vec.as_slice(),
                    current_checkpoint.checkpoint_number,
                ).await.unwrap();
                task_function.load_state(checkpoint_data).await;
            }

            output_manager.set_last_slot_ending_checkpoint(
                output_stream_id,
                output_generation_id,
                current_checkpoint.checkpoint_number,
            ).await;
            output_generation_id += 1;
            output_sink = Self::create_new_output_slot_generation(
                output_manager.clone(),
                output_stream_id,
                output_generation_id,
                next_partitions_vec.clone(),
                current_checkpoint.clone(),
            ).await;
        }

        let action_stream = Self::create_inputs_stream(
            &next_inputs,
            input_manager,
        ).await;

        (action_stream, output_generation_id, output_sink)
    }

    async fn create_inputs_stream(
        current_task_inputs: &TaskInputStreamGeneration,
        input_manager: &InputManager
    ) -> Option<MergedActionStream> {
        let streams_count = current_task_inputs.partition_range.len() * current_task_inputs.streams.iter().map(|stream| stream.addresses.len()).sum::<usize>();
        let mut streams = Vec::with_capacity(streams_count);
        for partition in &current_task_inputs.partition_range {
            for stream_def in &current_task_inputs.streams {
                for address in &stream_def.addresses {
                    let stream = input_manager.stream_input(
                        &address.address,
                        &address.stream_id,
                        *partition,
                        stream_def.input_schema.clone(),
                    ).await.unwrap();
                    streams.push((stream_def.ordinal, stream));
                }
            }
        }
        println!("Loaded {} input streams", streams.len());

        (!streams.is_empty()).then(||
            // TODO check that all the schemas match
            MergedActionStream::merge_inputs(streams)
        )
    }

    async fn create_new_output_slot_generation(
        output_manager: Arc<OutputManager>,
        output_stream_id: &str,
        generation_id: usize,
        partitions: Vec<usize>,
        starting_marker: Marker,
    ) -> OutputChannel {
        // Create a lot in the output for this task
        output_manager.append_slot_generation(
            output_stream_id,
            generation_id,
            partitions,
            starting_marker.checkpoint_number,
        ).await;

        let output_stream_id = output_stream_id.to_string();
        Box::new(move |item| {
            let output_manager = Arc::clone(&output_manager);
            let output_stream_id = output_stream_id.clone();
            Box::pin(async move {
                println!("Writing batch to output {}", output_stream_id);
                output_manager.write(&output_stream_id, generation_id, item).await.unwrap();
            }) as Pin<Box<dyn Future<Output=()> + Sync + Send>>
        })
    }

    async fn run_to_end_of_generation(
        task_id: &str,
        input_generation_index: usize,
        input_generations: &Mutex<Vec<TaskInputStreamGeneration>>,
        task_function: &mut (dyn TaskFunction + Send + Sync),
        action_stream: &mut Option<MergedActionStream>,
        partitions: &[usize],
        checkpoint_id: &str,
        output_sink: &mut OutputChannel,
        checkpoint_storage_manager: &CheckpointStorageManager,
    ) -> Result<(TaskInputStreamGeneration, usize, Marker), Option<Marker>> {
        let mut last_marker = None;
        loop {
            let maybe_marker = Self::run_to_next_marker(action_stream, task_function, output_sink).await;
            match maybe_marker {
                // Stream finished
                None => break,
                Some(marker) => {
                    println!("Task {} discovered marker {}", task_id, marker.checkpoint_number);
                    last_marker = Some(marker.clone());
                    checkpoint_storage_manager.write_checkpoint_state(
                        checkpoint_id.to_string(),
                        partitions,
                        marker.checkpoint_number,
                        task_function.get_state().await,
                    ).await.unwrap();

                    // Check if we can move to the next generation
                    let maybe_next_generation = Self::get_next_generation(
                        input_generation_index,
                        input_generations,
                        marker.clone(),
                    ).await;
                    if let Some((next_generation, next_index)) = maybe_next_generation {
                        return Ok((next_generation, next_index, marker));
                    }
                },
            }
        }

        Err(last_marker)
    }

    async fn get_next_generation(
        input_generation_index: usize,
        task_inputs: &Mutex<Vec<TaskInputStreamGeneration>>,
        marker: Marker,
    ) -> Option<(TaskInputStreamGeneration, usize)> {
        // Check if there is a new generation of input that we need to move to
        let task_inputs = task_inputs.lock().await;
        if let Some(next_generation) = task_inputs.get(input_generation_index + 1) {
            if marker.checkpoint_number >= next_generation.transition_after {
                Some((next_generation.clone(), input_generation_index + 1))
            } else {
                None
            }
        } else {
            None
        }
    }

    async fn run_to_next_marker<'a>(
        stream: &mut Option<MergedActionStream>,
        task_function: &'a mut (dyn TaskFunction + Send + Sync),
        output_sink: &'a mut OutputChannel,
    ) -> Option<Marker> {
        if let Some(stream) = stream {
            // Intermediate task
            while let Some(action) = stream.next().await {
                match action {
                    // Break the loop when we find a marker
                    Ok(OrdinalStreamItem::Marker(marker)) => {
                        println!("Task ran to marker {}", marker.checkpoint_number);
                        return Some(marker);
                    },
                    Ok(OrdinalStreamItem::RecordBatch(ordinal, batch)) => {
                        println!("Processing input batch");
                        let state = task_function.process(batch, ordinal, output_sink).await;
                        if state == TaskState::Exhausted {
                            println!("Task finished");
                            break;
                        }
                    },
                    Err(e) => {
                        // TODO handle error
                        panic!("Error reading from input stream: {:?}", e);
                    },
                }
            }
        } else {
            // Source task
            println!("Running source task");
            let mut discovered_marker = Arc::new(Mutex::new(None));
            let mut local_output_sink: OutputChannelL<'a> = {
                let discovered_marker = Arc::clone(&discovered_marker);
                Box::new(move |item| {
                    match item {
                        // Write the value to the output sink if it is not a marker
                        StreamItem::RecordBatch(batch) => {
                            output_sink(StreamItem::RecordBatch(batch))
                        },
                        StreamItem::Marker(marker) => {
                            let discovered_marker = Arc::clone(&discovered_marker);
                            Box::pin(async move {
                                let mut discovered_marker = discovered_marker.lock().await;
                                *discovered_marker = Some(marker);
                            })
                        },
                    }
                })
            };

            loop {
                let state = task_function.poll(&mut local_output_sink).await;

                // Check if we found a marker in the output of the sink function
                if let Some(marker) = discovered_marker.lock().await.take() {
                    println!("Source task emitted marker {}", marker.checkpoint_number);
                    return Some(marker);
                }

                if state == TaskState::Exhausted {
                    println!("Source task exhausted");
                    break;
                }

                println!("Source task iteration");
            }
        }

        None
    }

    // async fn run(
    //     task_id: String,
    //     task_inputs: Option<TaskInputs>,
    //     mut task_function: Box<dyn TaskFunction + Send + Sync>,
    //     task_output_partitions: usize,
    //     task_output_schema: SchemaRef,
    //     task_checkpoint: Option<String>,
    //     input_manager: Arc<InputManager>,
    //     output_manager: Arc<OutputManager>,
    //     checkpoint_storage_manager: Arc<CheckpointStorageManager>,
    // ) {
    //     println!("Starting task");
    //
    //     // Create a lot in the output for this task
    //     output_manager.create_slot(
    //         task_id.clone(),
    //         task_output_partitions,
    //         task_output_schema,
    //     ).await;
    //
    //     // Create a sink that will write to the output slot
    //     let mut output_sink: OutputChannel = {
    //         let output_manager = Arc::clone(&output_manager);
    //         let task_id = task_id.clone();
    //         Box::new(move |item| {
    //             let output_manager = Arc::clone(&output_manager);
    //             let task_id = task_id.clone();
    //             Box::pin(async move {
    //                 println!("Writing batch to output {}", task_id);
    //                 output_manager.write(&task_id, 0, item).await.unwrap();
    //             }) as Pin<Box<dyn Future<Output=()> + Sync + Send>>
    //         })
    //     };
    //
    //     // Initialize the task
    //     task_function.init().await;
    //
    //     // Check if we need to load the previous checkpoint
    //     if let Some(checkpoint) = task_checkpoint {
    //         let checkpoint_data = checkpoint_storage_manager.get_checkpoint_state(&checkpoint).await.unwrap();
    //         task_function.load_state(checkpoint_data).await;
    //     }
    //
    //     if let Some(task_inputs) = task_inputs {
    //         // Create input stream, feed it to the task function, and write the output to the output
    //         // manager.
    //         let mut streams = Vec::with_capacity(task_inputs.cells.len());
    //         println!("Loaded {} input streams", task_inputs.cells.len());
    //         for cell in task_inputs.cells {
    //             let stream = input_manager.stream_input(
    //                 &task_inputs.address,
    //                 &task_inputs.stream_id,
    //                 cell.partition,
    //             ).await.unwrap();
    //             streams.push(stream);
    //         }
    //
    //         // TODO check that all the schemas match
    //
    //         let mut action_stream = MergedActionStream::merge_inputs(streams);
    //         while let Some(action) = action_stream.next().await {
    //             match action {
    //                 Ok(StreamItem::Marker(marker)) => {
    //                     // Need to take a checkpoint
    //                     let checkpoint_data = task_function.get_state().await;
    //                     let checkpoint_id = format!("{}-{}", task_id, marker.checkpoint_number);
    //                     checkpoint_storage_manager.write_checkpoint_state(checkpoint_id, checkpoint_data).await;
    //                 },
    //                 Ok(StreamItem::RecordBatch(batch)) => {
    //                     println!("Processing input batch");
    //                     let state = task_function.process(batch, &mut output_sink).await;
    //                     if state == TaskState::Exhausted {
    //                         break;
    //                     }
    //                 },
    //                 Err(e) => {
    //                     // TODO handle error
    //                     panic!("Error reading from input stream: {:?}", e);
    //                 },
    //             }
    //         }
    //     } else {
    //         let mut checkpoint_number = 0;
    //         loop {
    //             println!("Polling source task");
    //             let state = task_function.poll(&mut output_sink).await;
    //             if state == TaskState::Exhausted {
    //                 break;
    //             }
    //
    //             // By default, take a snapshot after every poll
    //             let checkpoint_data = task_function.get_state().await;
    //             let checkpoint_id = format!("{}-{}", task_id, checkpoint_number);
    //             checkpoint_storage_manager.write_checkpoint_state(checkpoint_id, checkpoint_data).await;
    //
    //             output_manager.write(&task_id, 0, StreamItem::Marker(Marker { checkpoint_number })).await.unwrap();
    //             checkpoint_number += 1;
    //         }
    //     }
    //
    //     task_function.finish(&mut output_sink).await;
    //     output_manager.finish(&task_id).await.unwrap();
    // }
}

// struct InputPhaseStreamState {
//     ordinal: usize,
//     generations: Mutex<Vec<TaskInputStreamGeneration>>,
//     current_generation_index: usize,
// }

// struct InputPhaseState {
//     current_generation_index: usize,
//     generations: Mutex<Vec<TaskInputStreamGeneration>>,
//     // streams: Vec<InputPhaseStreamState>
// }

struct TaskInputPhaseState {
    generations: Mutex<Vec<TaskInputStreamGeneration>>,
}
