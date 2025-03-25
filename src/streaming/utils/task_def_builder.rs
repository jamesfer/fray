use crate::streaming::output_manager::OutputSlotPartitioning;
use crate::streaming::task_definition::{TaskDefinition, TaskInputDefinition, TaskInputPhase, TaskInputStream, TaskInputStreamAddress, TaskInputStreamGeneration, TaskSpec};
use crate::streaming::tasks::source::{SourceOperator, SourceTask};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use rand::{random, Rng};
use crate::streaming::tasks::identity::{IdentityOperator, IdentityTask};

pub struct TaskDefBuilder {
    // Required
    output_schema: SchemaRef,
    // Optional
    spec: Option<TaskSpec>,
    id: Option<String>,
    checkpoint_id: Option<String>,
    output_stream_id: Option<String>,
    output_partitioning: Option<Option<OutputSlotPartitioning>>,
    inputs: Option<TaskInputDefinition>,
}

impl TaskDefBuilder {
    pub fn new(
        output_schema: SchemaRef,
    ) -> Self {
        Self {
            output_schema,
            spec: None,
            id: None,
            checkpoint_id: None,
            output_stream_id: None,
            output_partitioning: None,
            inputs: None,
        }
    }

    pub fn identity(output_schema: SchemaRef) -> Self {
        Self::new(output_schema)
    }

    pub fn source(record_batches: Vec<RecordBatch>) -> Self {
        let schema = record_batches[0].schema();
        // TODO
        Self::new(schema).spec(TaskSpec::Source(SourceOperator::new(record_batches)))
    }

    pub fn id<S: Into<String>>(mut self, id: S) -> Self {
        self.id = Some(id.into());
        self
    }

    pub fn spec(mut self, task_spec: TaskSpec) -> Self {
        self.spec = Some(task_spec);
        self
    }

    pub fn checkpoint_id<S: Into<String>>(mut self, checkpoint_id: S) -> Self {
        self.checkpoint_id = Some(checkpoint_id.into());
        self
    }

    pub fn output_stream_id<S: Into<String>>(mut self, output_stream_id: S) -> Self {
        self.output_stream_id = Some(output_stream_id.into());
        self
    }

    pub fn output_partitioning(mut self, output_partitioning: Option<OutputSlotPartitioning>) -> Self {
        self.output_partitioning = Some(output_partitioning);
        self
    }

    pub fn inputs(mut self, inputs: TaskInputDefinition) -> Self {
        self.inputs = Some(inputs);
        self
    }

    // Single phase, single generation, single input stream, single address, single partition
    pub fn input_address<S1: Into<String>, S2: Into<String>>(mut self, address: S1, stream_id: S2, schema: SchemaRef) -> Self {
        self.inputs = Some(TaskInputDefinition {
            phases: vec![TaskInputPhase {
                generations: vec![TaskInputStreamGeneration {
                    transition_after: 0,
                    partition_range: vec![0],
                    streams: vec![TaskInputStream {
                        input_schema: schema,
                        ordinal: 0,
                        addresses: vec![TaskInputStreamAddress {
                            address: address.into(),
                            stream_id: stream_id.into(),
                        }],
                    }],
                }],
            }],
        });
        self
    }

    pub fn build(self) -> TaskDefinition {
        let id = self.id.unwrap_or_else(|| format!("task-{}", random::<u64>()));
        TaskDefinition {
            id: id.clone(),
            checkpoint_id: self.checkpoint_id.unwrap_or_else(|| format!("checkpoint-{}", id)),
            // TODO
            spec: self.spec.unwrap_or_else(|| TaskSpec::Identity(IdentityOperator)),
            output_stream_id: self.output_stream_id.unwrap_or_else(|| format!("output-stream-{}", id)),
            output_schema: self.output_schema,
            output_partitioning: self.output_partitioning.unwrap_or(None),
            // Defaults to a source task
            inputs: self.inputs.unwrap_or_else(|| TaskInputDefinition {
                phases: vec![TaskInputPhase {
                    generations: vec![TaskInputStreamGeneration {
                        transition_after: 0,
                        partition_range: vec![0],
                        streams: vec![],
                    }],
                }],
            }),
        }
    }
}
