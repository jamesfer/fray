use crate::streaming::operators::identity::IdentityOperator;
use crate::streaming::operators::source::SourceOperator;
use crate::streaming::operators::task_function::{CreateOperatorFunction2, OperatorFunction2};

#[derive(Clone)]
pub enum OperatorSpec {
    Identity(IdentityOperator),
    Source(SourceOperator),
}

impl CreateOperatorFunction2 for OperatorSpec {
    // type OperatorFunctionType = Box<dyn OperatorFunction2 + Sync + Send>;

    fn create_operator_function(&self) -> Box<dyn OperatorFunction2 + Sync + Send> {
        match self {
            OperatorSpec::Source(source) => source.create_operator_function(),
            OperatorSpec::Identity(identity) => identity.create_operator_function(),
        }
    }
}

#[derive(Clone)]
pub struct OperatorInput {
    pub stream_id: String,
    pub ordinal: usize,
}

#[derive(Clone)]
pub struct OperatorOutput {
    pub stream_id: String,
    pub ordinal: usize,
}

#[derive(Clone)]
pub struct OperatorDefinition {
    pub id: String,
    pub state_id: String,
    pub spec: OperatorSpec,
    pub inputs: Vec<OperatorInput>,
    pub outputs: Vec<OperatorOutput>,
}