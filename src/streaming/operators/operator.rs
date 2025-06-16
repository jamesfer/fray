use serde::{Deserialize, Serialize};
use crate::streaming::operators::count_star::CountStarOperator;
use crate::streaming::operators::identity::IdentityOperator;
use crate::streaming::operators::nested::NestedOperator;
use crate::streaming::operators::remote_exchange::RemoteExchangeOperator;
use crate::streaming::operators::remote_source::remote_source::RemoteSourceOperator;
use crate::streaming::operators::source::SourceOperator;
use crate::streaming::operators::task_function::{CreateOperatorFunction2, OperatorFunction2};

#[derive(Clone, Serialize, Deserialize)]
pub enum OperatorSpec {
    Identity(IdentityOperator),
    Source(SourceOperator),
    CountStar(CountStarOperator),
    Nested(NestedOperator),
    RemoteExchangeOutput(RemoteExchangeOperator),
    RemoteExchangeInput(RemoteSourceOperator),
}

impl CreateOperatorFunction2 for OperatorSpec {
    fn create_operator_function(&self) -> Box<dyn OperatorFunction2 + Sync + Send> {
        match self {
            OperatorSpec::Identity(op) => op.create_operator_function(),
            OperatorSpec::Source(op) => op.create_operator_function(),
            OperatorSpec::CountStar(op) => op.create_operator_function(),
            OperatorSpec::Nested(op) => op.create_operator_function(),
            OperatorSpec::RemoteExchangeOutput(op) => op.create_operator_function(),
            OperatorSpec::RemoteExchangeInput(op) => op.create_operator_function(),
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct OperatorInput {
    pub stream_id: String,
    pub ordinal: usize,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct OperatorOutput {
    pub stream_id: String,
    pub ordinal: usize,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct OperatorDefinition {
    pub id: String,
    pub state_id: String,
    pub spec: OperatorSpec,
    pub inputs: Vec<OperatorInput>,
    pub outputs: Vec<OperatorOutput>,
}
