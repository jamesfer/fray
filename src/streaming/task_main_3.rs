use std::sync::Arc;
use datafusion::common::internal_datafusion_err;
use datafusion::error::DataFusionError;
use crate::streaming::operators::operator::OperatorDefinition;
use crate::streaming::operators::task_function::{CreateOperatorFunction2, OperatorFunction2};
use crate::streaming::runtime::Runtime;

pub async fn task_main(
    task_id: String,
    operator: OperatorDefinition,
    runtime: Arc<Runtime>,
) -> Result<(), DataFusionError> {
    if operator.inputs.len() > 0 || operator.outputs.len() > 0 {
        return Err(internal_datafusion_err!("Task main doesn't support operators with inputs and outputs"));
    }

    println!("Starting task {}", task_id);
    let mut function = operator.spec.create_operator_function();

    function.init(runtime).await;
    
    let outputs = function.run(vec![]).await;
    assert_eq!(outputs.len(), 0);

    Ok(())
}
