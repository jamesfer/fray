use crate::python::py_task_definition::DFRayTaskDefinition;
use crate::streaming::operators::nested::NestedOperator;
use crate::streaming::operators::operator::{OperatorDefinition, OperatorInput, OperatorOutput, OperatorSpec};
use crate::streaming::operators::remote_exchange::RemoteExchangeOperator;
use crate::streaming::operators::remote_source::remote_source::RemoteSourceOperator;
use crate::streaming::operators::source::SourceOperator;
use crate::streaming::task_definition_2::TaskDefinition2;
use datafusion::common::record_batch;
use futures_util::{StreamExt, TryStreamExt};
use pyo3::{pyfunction, PyResult, Python};
use std::sync::Arc;

use crate::python::py_utils::collect_inner;
use crate::streaming::coordinator::{ComputeEnv, PythonResources, StaticCoordinator};
use pyo3::prelude::*;

#[pyfunction]
pub fn entrypoint(
    processor_class: PyObject,
    ray_get: PyObject,
) -> PyResult<()> {
    let rt = tokio::runtime::Runtime::new()
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to create tokio runtime: {}", e)))?;
    
    let python_resources = Arc::new(PythonResources::new(processor_class, ray_get)?);
    rt.block_on(async_entrypoint(python_resources))
}

async fn async_entrypoint(
    python_resources: Arc<PythonResources>,
) -> PyResult<()> {
    let runtime_env = Arc::new(ComputeEnv::new(python_resources.clone()));
    let coordinator = StaticCoordinator::new(runtime_env.clone());

    let tasks = get_tasks_inner();
    let running_tasks = coordinator.schedule_together(&tasks).await?;

    let (output_task, output_addr, _processor) = running_tasks.last()
        .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("No tasks assigned"))?;
    let results = collect_inner(output_addr, output_task.exchange_outputs().last().unwrap()).await
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("Error collecting results: {}", e)))?;

    println!("Results collected: {:?}", results);
    Ok(())
}

fn get_tasks_inner() -> Vec<TaskDefinition2> {
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
    vec![source_task, exchange_task]
}

#[pyfunction]
pub fn get_tasks(py: Python) -> PyResult<Vec<DFRayTaskDefinition>> {
    Ok(get_tasks_inner()
        .into_iter()
        .map(|task| {
            DFRayTaskDefinition {
                task_definition2: task,
            }
        })
        .collect())
}
