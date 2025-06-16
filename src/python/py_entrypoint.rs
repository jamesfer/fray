use crate::python::py_task_definition::DFRayTaskDefinition;
use crate::streaming::operators::nested::NestedOperator;
use crate::streaming::operators::operator::{OperatorDefinition, OperatorInput, OperatorOutput, OperatorSpec};
use crate::streaming::operators::remote_exchange::RemoteExchangeOperator;
use crate::streaming::operators::source::SourceOperator;
use crate::streaming::task_definition_2::TaskDefinition2;
use datafusion::common::record_batch;
use futures_util::{StreamExt, TryStreamExt};
use pyo3::{pyfunction, PyResult, Python};
use crate::streaming::operators::remote_source::remote_source::RemoteSourceOperator;

#[pyfunction]
pub fn get_tasks(py: Python) -> PyResult<Vec<DFRayTaskDefinition>> {
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

    Ok(vec![
        DFRayTaskDefinition {
            task_definition2: source_task,
        },
        DFRayTaskDefinition {
            task_definition2: exchange_task,
        },
    ])
}
