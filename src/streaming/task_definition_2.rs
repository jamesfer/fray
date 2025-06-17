use std::borrow::Cow;
use serde::{Deserialize, Serialize};
use datafusion::common::{internal_datafusion_err, DataFusionError};
use crate::streaming::operators::operator::{OperatorDefinition, OperatorSpec};

#[derive(Clone, Serialize, Deserialize)]
pub struct TaskDefinition2 {
    pub task_id: String,
    pub operator: OperatorDefinition,
}

impl TaskDefinition2 {
    pub fn to_bytes(&self) -> Result<Cow<[u8]>, DataFusionError> {
        match flexbuffers::to_vec(&self) {
            Ok(bytes) => Ok(Cow::Owned(bytes)),
            Err(e) => Err(internal_datafusion_err!(
                "Failed to serialize TaskDefinition2 to Flexbuffer: {}", e
            )),
        }
    }

    pub fn exchange_outputs(&self) -> Vec<String> {
        Self::get_exchange_outputs(&self.operator.spec)
    }

    pub fn exchange_inputs(&self) -> Vec<String> {
        Self::get_exchange_inputs(&self.operator.spec)
    }

    fn get_exchange_outputs(operator: &OperatorSpec) -> Vec<String> {
        match operator {
            OperatorSpec::Identity(_) => vec![],
            OperatorSpec::Source(_) => vec![],
            OperatorSpec::CountStar(_) => vec![],
            OperatorSpec::RemoteExchangeInput(_) => vec![],
            OperatorSpec::RemoteExchangeOutput(output) => vec![output.get_stream_id().to_string()],
            OperatorSpec::Nested(nested) => nested.get_operators().iter()
                .flat_map(|op| Self::get_exchange_outputs(&op.spec))
                .collect(),
        }
    }

    fn get_exchange_inputs(operator: &OperatorSpec) -> Vec<String> {
        match operator {
            OperatorSpec::Identity(_) => vec![],
            OperatorSpec::Source(_) => vec![],
            OperatorSpec::CountStar(_) => vec![],
            OperatorSpec::RemoteExchangeOutput(_) => vec![],
            OperatorSpec::RemoteExchangeInput(input) => input.get_stream_ids().iter().cloned().collect(),
            OperatorSpec::Nested(nested) => nested.get_operators().iter()
                .flat_map(|op| Self::get_exchange_inputs(&op.spec))
                .collect(),
        }
    }
}
