use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use datafusion::error::DataFusionError;
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::values::ValuesExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use std::collections::HashMap;
use std::fmt::Display;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use arrow::datatypes::SchemaRef;
use datafusion::common::internal_datafusion_err;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::union::UnionExec;
use crate::stage::DFRayStageExec;
use crate::streaming::task_definition::{TaskDefinition, TaskInputDefinition, TaskInputPhase, TaskInputStream, TaskInputStreamAddress, TaskInputStreamGeneration, TaskSpec};
use crate::streaming::utils::task_def_builder::TaskDefBuilder;
use crate::streaming::tasks::projection::{ProjectionExpression, ProjectionOperator, ProjectionTask};
use crate::streaming::tasks::filter::{FilterOperator, FilterTask};
use crate::streaming::tasks::union::UnionOperator;

// A kind of hacky way to uniquely identify Arc values
struct Holder<T: ?Sized> {
    pointer: usize,
    arc: Arc<T>,
}

impl <T: ?Sized> Holder<T> {
    fn new(arc: Arc<T>) -> Self {
        Self {
            pointer: Arc::as_ptr(&arc) as *const () as usize,
            arc,
        }
    }
}

impl <T: ?Sized> Hash for Holder<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_usize(self.pointer);
    }
}

impl<T: ?Sized> PartialEq<Self> for Holder<T> {
    fn eq(&self, other: &Self) -> bool {
        self.pointer == other.pointer
    }
}

impl <T: ?Sized> Eq for Holder<T> {}

enum TaskConversion {
    Converted(String),
    Skipped(String),
}

impl TaskConversion {
    fn get_id(&self) -> &str {
        match self {
            TaskConversion::Converted(id) => id,
            TaskConversion::Skipped(id) => id,
        }
    }
}

struct Visitor {
    node_lookup: HashMap<Holder<dyn ExecutionPlan>, TaskConversion>,
    tasks: HashMap<String, TaskDefinition>,
    counter: u64,
}

impl Visitor {
    fn new() -> Self {
        Self {
            node_lookup: HashMap::new(),
            tasks: HashMap::new(),
            counter: 0,
        }
    }

    fn into_tasks(self) -> impl Iterator<Item = TaskDefinition> {
        self.tasks.into_values()
    }

    fn get_converted_task(&mut self, exec_node: &Arc<dyn ExecutionPlan>) -> Result<&TaskDefinition, DataFusionError> {
        let conversion_result = self.node_lookup.get(&Holder::new(Arc::clone(exec_node)))
            .ok_or_else(|| DataFusionError::Internal("Input not found".to_string()))?;
        let converted_task = self.tasks.get(conversion_result.get_id())
            .ok_or_else(|| DataFusionError::Internal("Input task not found".to_string()))?;
        Ok(converted_task)
    }

    fn next_name(&mut self, prefix: impl Display) -> String {
        let name = format!("{}-{}", prefix, self.counter);
        self.counter += 1;
        name
    }
}

#[macro_export]
macro_rules! downcast_match {
    ( $x:expr, $( $t:ty : $i:ident -> $b:block ),+,) => {
        {
            $(
                if let Some($i) = $x.downcast_ref::<$t>() {
                    Some($b)
                } else
            )*
            {
                None
            }
        }
    };
}


impl TreeNodeVisitor<'_> for Visitor {
    type Node = Arc<dyn ExecutionPlan>;

    fn f_up(&mut self, node: &Self::Node) -> Result<TreeNodeRecursion, DataFusionError> {
        let any_node = node.as_any();

        let conversion_result = downcast_match!{
            any_node,
            ProjectionExec: projection_exec -> {
                let self_id = self.next_name("projection");
                let input_task = self.get_converted_task(projection_exec.input())?;

                let input_schema = projection_exec.input().schema();
                let expressions = projection_exec.expr().iter()
                    .map(|(expr, alias)| ProjectionExpression {
                        expression: expr.clone(),
                        alias: alias.clone(),
                    })
                    .collect();
                let task = TaskDefBuilder::new(projection_exec.schema())
                    .id(self_id.clone())
                    .spec(TaskSpec::Projection(ProjectionOperator {
                        schema: input_schema.clone(),
                        expressions,
                    }))
                    .input_address("__PLACEHOLDER__", input_task.output_stream_id.clone(), input_schema.clone())
                    .build();

                self.tasks.insert(self_id.to_string(), task);
                TaskConversion::Converted(self_id.to_string())
            },
            FilterExec: filter_exec -> {
                // Validate
                if filter_exec.projection().is_some() {
                    return Err(DataFusionError::Internal("FilterExec cannot have a projection".to_string()));
                }

                let self_id = self.next_name("filter");
                let input_task = self.get_converted_task(filter_exec.input())?;

                let input_schema = filter_exec.input().schema();
                let task = TaskDefBuilder::new(filter_exec.schema())
                    .id(self_id.clone())
                    .spec(TaskSpec::Filter(FilterOperator::new(input_schema.clone(), filter_exec.predicate().clone())))
                    .input_address("__PLACEHOLDER__", input_task.output_stream_id.clone(), input_schema)
                    .build();

                self.tasks.insert(self_id.to_string(), task);
                TaskConversion::Converted(self_id.to_string())
            },
            MemoryExec: memory_exec -> {
                // Validate
                if memory_exec.projection().is_some() {
                    return Err(internal_datafusion_err!("MemoryExec cannot have a projection"));
                }
                if memory_exec.partitions().len() != 1 {
                    return Err(internal_datafusion_err!("Invalid number of projections: {}", memory_exec.partitions().len()));
                }
                if memory_exec.sort_information().len() > 0 {
                    return Err(internal_datafusion_err!("Don't support sorting"));
                }

                let self_id = self.next_name("memory");
                let batches = &memory_exec.partitions()[0];
                let task = TaskDefBuilder::source(batches.clone())
                    .id(self_id.clone())
                    .build();

                self.tasks.insert(self_id.to_string(), task);
                TaskConversion::Converted(self_id.to_string())
            },
            UnionExec: union_exec -> {
                let input_schema = union_exec.inputs()[0].schema();
                let streams = union_exec.inputs().iter()
                    .enumerate()
                    .map(|(ordinal, input)| -> Result<_, DataFusionError>{
                        let input_task = self.get_converted_task(input)?;
                        Ok(TaskInputStream {
                            ordinal,
                            addresses: vec![TaskInputStreamAddress {
                                address: "blah".to_string(),
                                stream_id: input_task.output_stream_id.clone(),
                            }],
                            input_schema: input_schema.clone(),
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                let self_id = self.next_name("union");
                let task = TaskDefBuilder::new(input_schema)
                    .id(self_id.clone())
                    .spec(TaskSpec::Union(UnionOperator))
                    .inputs(TaskInputDefinition {
                        phases: vec![TaskInputPhase {
                            generations: vec![TaskInputStreamGeneration {
                                streams,
                                transition_after: 0,
                                partition_range: vec![0],
                            }],
                        }],
                    })
                    .build();

                self.tasks.insert(self_id.to_string(), task);
                TaskConversion::Converted(self_id.to_string())
            },
            CoalesceBatchesExec: coalesce_batches_exec -> {
                let input_task = self.get_converted_task(coalesce_batches_exec.input())?;
                TaskConversion::Skipped((&input_task.id).clone())
            },
            DFRayStageExec: ray_stage_exec -> {
                // Ignore these stages inserted by ray
                let input_task = self.get_converted_task(&ray_stage_exec.input)?;
                TaskConversion::Skipped((&input_task.id).clone())
            },
            ValuesExec: _values_exec -> {
                Err(internal_datafusion_err!("ValuesExec is deprecated"))?
            },
        }.ok_or_else(|| DataFusionError::Internal(format!("Unsupported exec node: {}", node.name())))?;

        self.node_lookup.insert(Holder::new(node.clone()), conversion_result);
        Ok(TreeNodeRecursion::Continue)
    }
}

pub fn translate_physical_plan(plan: &Arc<dyn ExecutionPlan>) -> Result<Vec<TaskDefinition>, DataFusionError> {
    let mut visitor = Visitor::new();
    plan.visit(&mut visitor)?;
    Ok(visitor.into_tasks().collect())
}

async fn create_physical_datafusion_plan(sql: &str) -> Arc<dyn ExecutionPlan> {
    let context = SessionContext::new();
    let logical_plan = context.state().create_logical_plan(sql).await.unwrap();
    context.state().create_physical_plan(&logical_plan).await.unwrap()
}

pub async fn translate_sql_to_tasks(sql: &str) -> Vec<TaskDefinition> {
    let physical_plan = create_physical_datafusion_plan(sql).await;
    translate_physical_plan(&physical_plan).unwrap()
}


#[cfg(test)]
mod tests {
    use datafusion::physical_plan::displayable;
    use crate::streaming::translation::translate_physical_plan::{create_physical_datafusion_plan, translate_physical_plan};

    #[tokio::test]
    async fn can_translate_simple_query() {
        let sql = "
            select *
            from values (1), (2), (3)
            where \"column1\" > 2
        ";

        let physical_plan = create_physical_datafusion_plan(sql).await;
        println!("Plan: {}", displayable(physical_plan.as_ref()).indent(true));

        let tasks = translate_physical_plan(&physical_plan).unwrap();
        assert_eq!(tasks.len(), 2);
    }

}
