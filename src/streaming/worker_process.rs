use crate::flight::FlightHandler;
use crate::streaming::generation::{GenerationInputDetail, GenerationSpec, TaskSchedulingDetailsUpdate};
use crate::streaming::runtime::Runtime;
use crate::streaming::task_definition_2::TaskDefinition2;
use crate::streaming::task_main_3::RunningTask;
use datafusion::common::internal_datafusion_err;
use datafusion::error::DataFusionError;
use local_ip_address::local_ip;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone, Serialize, Deserialize)]
pub struct InitialSchedulingDetails {
    pub generations: Vec<GenerationSpec>,
    pub input_locations: Vec<GenerationInputDetail>,
}

pub struct WorkerProcess {
    name: String,
    runtime: Arc<Runtime>,
    running_tasks: RwLock<HashMap<String, RunningTask>>,
}

impl WorkerProcess {
    pub async fn start(name: String) -> Result<Self, DataFusionError> {
        let name = format!("[{}]", name);
        let local_ip_addr = local_ip()
            .map_err(|err| internal_datafusion_err!("Failed to get worker local ip: {}", err))?;
        let runtime = Arc::new(Runtime::start(local_ip_addr).await?);

        Ok(Self {
            name,
            runtime,
            running_tasks: RwLock::new(HashMap::new()),
        })
    }

    pub fn data_exchange_address(&self) -> &str {
        self.runtime.data_exchange_manager().get_exchange_address()
    }

    pub async fn start_task(
        &self,
        task_definition: TaskDefinition2,
        initial_scheduling_details: InitialSchedulingDetails,
    ) -> Result<(), DataFusionError> {
        let running_task = RunningTask::start(
            task_definition.task_id.clone(),
            task_definition.operator,
            self.runtime.clone(),
            0, // initial checkpoint
            initial_scheduling_details.input_locations,
            initial_scheduling_details.generations,
        ).await?;

        let mut running_tasks = self.running_tasks.write();
        match running_tasks.entry(task_definition.task_id.clone()) {
            Entry::Occupied(_) => {
                running_task.cancel();
                Err(internal_datafusion_err!(
                    "Task with ID {} is already running",
                    task_definition.task_id
                ))
            }
            Entry::Vacant(entry) => {
                entry.insert(running_task);
                Ok(())
            },
        }
    }

    pub async fn update_input(&self, task_id: String, task_scheduling_details_update: TaskSchedulingDetailsUpdate) -> Result<(), DataFusionError> {
        let running_tasks = self.running_tasks.read();
        match running_tasks.get(&task_id) {
            None => {
                Err(internal_datafusion_err!(
                    "No running task found with ID {}",
                    task_id
                ))
            }
            Some(running_task) => {
                running_task.update_scheduling_details(task_scheduling_details_update).await;
                Ok(())
            }
        }
    }

    #[cfg(test)]
    pub fn get_runtime(&self) -> Arc<Runtime> {
        self.runtime.clone()
    }
}

#[cfg(test)]
mod tests {
    use crate::streaming::action_stream::{Marker, StreamItem};
    use crate::streaming::generation::{GenerationInputDetail, GenerationInputLocation, GenerationSpec};
    use crate::streaming::operators::nested::NestedOperator;
    use crate::streaming::operators::operator::{OperatorDefinition, OperatorInput, OperatorOutput, OperatorSpec};
    use crate::streaming::operators::remote_exchange::RemoteExchangeOperator;
    use crate::streaming::operators::remote_source::remote_source::RemoteSourceOperator;
    use crate::streaming::operators::source::SourceOperator;
    use crate::streaming::operators::task_function::SItem;
    use crate::streaming::task_definition_2::TaskDefinition2;
    use crate::streaming::utils::create_remote_stream::{create_remote_stream, create_remote_stream_no_runtime};
    use crate::streaming::utils::retry::retry_future;
    use crate::streaming::worker_process::{InitialSchedulingDetails, WorkerProcess};
    use datafusion::common::record_batch;
    use futures::StreamExt;

    #[tokio::test]
    pub async fn single_output_task() {
        let worker = WorkerProcess::start("worker1".to_string()).await.unwrap();

        let test_batch = record_batch!(
            ("a", Int32, vec![1i32, 2, 3])
        ).unwrap();
        let task_definition = TaskDefinition2 {
            task_id: "task1".to_string(),
            operator: OperatorDefinition {
                id: "nested1".to_string(),
                state_id: "1".to_string(),
                spec: OperatorSpec::Nested(NestedOperator::new(
                    vec![],
                    vec![
                        OperatorDefinition {
                            id: "source1".to_string(),
                            state_id: "2".to_string(),
                            spec: OperatorSpec::Source(SourceOperator::new(vec![test_batch.clone()])),
                            inputs: vec![],
                            outputs: vec![OperatorOutput {
                                stream_id: "output1".to_string(),
                                ordinal: 0,
                            }],
                        },
                        OperatorDefinition {
                            id: "exchange1".to_string(),
                            state_id: "3".to_string(),
                            spec: OperatorSpec::RemoteExchangeOutput(RemoteExchangeOperator::new("exchange_output1".to_string())),
                            inputs: vec![OperatorInput {
                                stream_id: "output1".to_string(),
                                ordinal: 0,
                            }],
                            outputs: vec![],
                        },
                    ],
                    vec![],
                )),
                inputs: vec![],
                outputs: vec![],
            },
        };

        worker.start_task(
            task_definition,
            InitialSchedulingDetails {
                generations: vec![GenerationSpec {
                    id: "gen1".to_string(),
                    partitions: vec![0],
                    start_conditions: vec![],
                }],
                input_locations: vec![],
            },
        ).await.unwrap();

        // Pull the output of the exchange operator
        let address = worker.data_exchange_address();
        let runtime = worker.get_runtime();

        let results_stream = retry_future(5, || {
            create_remote_stream(
                &runtime,
                "exchange_output1",
                address,
                vec![0],
            )
        }).await.unwrap();
        let results = Box::into_pin(results_stream).take(1).collect::<Vec<_>>().await;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].as_ref().unwrap(), &SItem::RecordBatch(test_batch));
    }

    #[tokio::test]
    pub async fn two_linked_tasks() {
        let worker1 = WorkerProcess::start("worker1".to_string()).await.unwrap();
        let worker2 = WorkerProcess::start("worker2".to_string()).await.unwrap();

        let test_batch = record_batch!(
            ("a", Int32, vec![1i32, 2, 3])
        ).unwrap();
        let source_task = TaskDefinition2 {
            task_id: "task1".to_string(),
            operator: OperatorDefinition {
                id: "nested1".to_string(),
                state_id: "1".to_string(),
                spec: OperatorSpec::Nested(NestedOperator::new(
                    vec![],
                    vec![
                        OperatorDefinition {
                            id: "source1".to_string(),
                            state_id: "2".to_string(),
                            spec: OperatorSpec::Source(SourceOperator::new_with_markers(vec![
                                StreamItem::Marker(Marker { checkpoint_number: 1 }),
                                StreamItem::RecordBatch(test_batch.clone()),
                                StreamItem::Marker(Marker { checkpoint_number: 2 }),
                            ])),
                            inputs: vec![],
                            outputs: vec![OperatorOutput {
                                stream_id: "output1".to_string(),
                                ordinal: 0,
                            }],
                        },
                        OperatorDefinition {
                            id: "exchange1".to_string(),
                            state_id: "3".to_string(),
                            spec: OperatorSpec::RemoteExchangeOutput(RemoteExchangeOperator::new("exchange_output1".to_string())),
                            inputs: vec![OperatorInput {
                                stream_id: "output1".to_string(),
                                ordinal: 0,
                            }],
                            outputs: vec![],
                        },
                    ],
                    vec![],
                )),
                inputs: vec![],
                outputs: vec![],
            },
        };
        let exchange_task = TaskDefinition2 {
            task_id: "task2".to_string(),
            operator: OperatorDefinition {
                id: "nested2".to_string(),
                state_id: "4".to_string(),
                spec: OperatorSpec::Nested(NestedOperator::new(
                    vec![],
                    vec![
                        OperatorDefinition {
                            id: "exchange2".to_string(),
                            state_id: "5".to_string(),
                            spec: OperatorSpec::RemoteExchangeInput(RemoteSourceOperator::new(vec!["exchange_output1".to_string()])),
                            inputs: vec![],
                            outputs: vec![OperatorOutput {
                                stream_id: "output2".to_string(),
                                ordinal: 0,
                            }],
                        },
                        OperatorDefinition {
                            id: "exchange3".to_string(),
                            state_id: "6".to_string(),
                            spec: OperatorSpec::RemoteExchangeOutput(RemoteExchangeOperator::new("exchange_output2".to_string())),
                            inputs: vec![OperatorInput {
                                stream_id: "output2".to_string(),
                                ordinal: 0,
                            }],
                            outputs: vec![],
                        },
                    ],
                    vec![],
                )),
                inputs: vec![],
                outputs: vec![],
            },
        };

        let address1 = worker1.data_exchange_address();
        let address2 = worker2.data_exchange_address();
        worker1.start_task(
            source_task,
            InitialSchedulingDetails {
                generations: vec![GenerationSpec {
                    id: "gen1".to_string(),
                    partitions: vec![0],
                    start_conditions: vec![],
                }],
                input_locations: vec![],
            },
        ).await.unwrap();
        worker2.start_task(
            exchange_task,
            InitialSchedulingDetails {
                generations: vec![GenerationSpec {
                    id: "gen1".to_string(),
                    partitions: vec![0],
                    start_conditions: vec![],
                }],
                input_locations: vec![GenerationInputDetail {
                    stream_id: "exchange_output1".to_string(),
                    locations: vec![GenerationInputLocation {
                        address: address1.to_string(),
                        offset_range: (0, 2 << 31),
                        partitions: vec![0],
                    }],
                }],
            },
        ).await.unwrap();

        // Pull the output of the exchange operator
        let results_stream = retry_future(5, || {
            create_remote_stream_no_runtime(
                "exchange_output2",
                address2,
                vec![0],
            )
        }).await.unwrap();
        let results = Box::into_pin(results_stream).take(3).collect::<Vec<_>>().await;

        assert_eq!(results.len(), 3);
        assert_eq!(results[0].as_ref().unwrap(), &SItem::Marker(Marker { checkpoint_number: 1 }));
        assert_eq!(results[1].as_ref().unwrap(), &SItem::RecordBatch(test_batch));
        assert_eq!(results[2].as_ref().unwrap(), &SItem::Marker(Marker { checkpoint_number: 2 }));
    }
}
