use datafusion::arrow::datatypes::SchemaRef;
use crate::output_manager::OutputSlotPartitioning;
use crate::task_function::TaskFunction;
use crate::tasks::projection::ProjectionTaskSpec;

#[derive(Clone)]
pub struct TaskInputStreamAddress {
    pub address: String,
    pub stream_id: String,
}

// #[derive(Clone)]
// pub struct TaskInputSStream {
//     pub addresses: Vec<TaskInputStreamAddress>,
//     pub partition_range: Vec<usize>,
// }


// #[derive(Clone)]
// pub enum TaskInputEvaluationOrder {
//     Parallel,
//     Sequential,
// }

#[derive(Clone)]
pub struct TaskInputStream {
    pub ordinal: usize,
    // pub generations: Vec<TaskInputStreamGeneration>,
    pub addresses: Vec<TaskInputStreamAddress>,
}

#[derive(Clone)]
pub struct TaskInputStreamGeneration {
    // pub addresses: Vec<TaskInputStreamAddress>,
    pub streams: Vec<TaskInputStream>,
    pub transition_after: u64,
    pub partition_range: Vec<usize>,
}

#[derive(Clone)]
pub struct TaskInputPhase {
    // pub streams: Vec<TaskInputStream>,
    pub generations: Vec<TaskInputStreamGeneration>,
}

#[derive(Clone)]
pub struct TaskInputDefinition {
    pub phases: Vec<TaskInputPhase>,
}




// pub struct TaskInputStream {
//     pub addresses: Vec<TaskInputStreamAddress>,
//     pub partition_range: Vec<usize>,
// }
//
// pub struct TaskInputGen {
//     pub transition_after: u64,
//     pub streams: Vec<TaskInputStream>,
// }
//
// pub struct TaskInputDef {
//     pub generations: Vec<TaskInputGeneration>,
//     pub order: TaskInputEvaluationOrder,
// }

pub enum TaskFunctionSpec {
    Projection(ProjectionTaskSpec),
}

impl Into<Box<dyn TaskFunction + Send + Sync>> for TaskFunctionSpec {
    fn into(self) -> Box<dyn TaskFunction + Send + Sync> {
        todo!()
    }
}

pub struct TaskDefinition {
    pub id: String,
    pub checkpoint_id: String,
    pub inputs: TaskInputDefinition,
    pub function: Box<dyn TaskFunction + Send + Sync>,
    pub output_stream_id: String,
    pub output_schema: SchemaRef,
    pub output_partitioning: Option<OutputSlotPartitioning>,
}
