use serde::{Deserialize, Serialize};
use crate::streaming::operators::operator::OperatorDefinition;

#[derive(Serialize, Deserialize)]
pub struct TaskDefinition2 {
    pub task_id: String,
    pub operator: OperatorDefinition,
}
