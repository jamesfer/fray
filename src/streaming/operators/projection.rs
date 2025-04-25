use crate::proto::generated::streaming_tasks as proto;
use crate::streaming::action_stream::StreamItem;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::common::{internal_datafusion_err, DataFusionError};
use datafusion::physical_expr::PhysicalExprRef;
use datafusion::physical_plan::streaming_operators::projection::ProjectionStreamingTask;
use datafusion::prelude::SessionContext;
use std::sync::Arc;
use async_trait::async_trait;
use crate::streaming::operators::serialization::{ProtoSerializer, P};
use crate::streaming::operators::task_function::{OutputChannel, OutputChannelL, TaskFunction, TaskState};

#[derive(Clone)]
pub struct ProjectionExpression {
    pub expression: PhysicalExprRef,
    pub alias: String,
}

impl ProtoSerializer for ProjectionExpression {
    type ProtoType = proto::ProjectionExpression;
    type SerializerContext<'a> = ();
    type DeserializerContext<'a> = (&'a SessionContext, &'a Schema);

    fn try_into_proto(self, context: &Self::SerializerContext<'_>) -> Result<Self::ProtoType, DataFusionError> {
        Ok(Self::ProtoType {
            expression: Some(self.expression.try_into_proto(context)?),
            alias: self.alias,
        })
    }

    fn try_from_proto(proto: Self::ProtoType, context: &Self::DeserializerContext<'_>) -> Result<Self, DataFusionError> {
        Ok(Self {
            expression: proto.expression
                .ok_or(internal_datafusion_err!("Expression is required for ProjectionExpression"))?
                .try_from_proto(context)?,
            alias: proto.alias,
        })
    }
}

#[derive(Clone)]
pub struct ProjectionOperator {
    pub schema: SchemaRef,
    pub expressions: Vec<ProjectionExpression>,
}

impl ProjectionOperator {
    pub fn into_function(self) -> ProjectionTask {
        ProjectionTask::new(
            self.expressions.into_iter().map(|expr| (expr.expression, expr.alias)).collect(),
            self.schema,
        )
    }
}

impl ProtoSerializer for ProjectionOperator {
    type ProtoType = proto::ProjectionOperator;
    type SerializerContext<'a> = ();
    type DeserializerContext<'a> = SessionContext;

    fn try_into_proto(self, context: &Self::SerializerContext<'_>) -> Result<Self::ProtoType, DataFusionError> {
        Ok(Self::ProtoType {
            input_schema: Some(self.schema.try_into_proto(context)?),
            expressions: self.expressions.try_into_proto(context)?,
        })
    }

    fn try_from_proto(proto: Self::ProtoType, context: &Self::DeserializerContext<'_>) -> Result<Self, DataFusionError> {
        let input_schema: SchemaRef = proto.input_schema
            .ok_or(internal_datafusion_err!("Schema is required for ProjectionOperator"))?
            .try_from_proto(&())?;
        let context = (context, input_schema.as_ref());
        let expressions = proto.expressions.try_from_proto(&context)?;
        Ok(Self {
            schema: input_schema,
            expressions,
        })
    }
}

pub struct ProjectionTask {
    inner: ProjectionStreamingTask,
}

impl ProjectionTask {
    pub fn new(expressions: Vec<(PhysicalExprRef, String)>, input_schema: SchemaRef) -> Self {
        Self {
            inner: ProjectionStreamingTask::new(expressions, input_schema).unwrap(),
        }
    }
}

#[async_trait]
impl TaskFunction for ProjectionTask {
    async fn init(&mut self) {}

    async fn poll(&mut self, _output: &mut OutputChannelL) -> TaskState {
        unimplemented!()
    }

    async fn process(&mut self, data: RecordBatch, _input_channel: usize, output: &mut OutputChannel) -> TaskState {
        output(StreamItem::RecordBatch(self.inner.process_batch(&data).unwrap())).await;
        TaskState::Continue
    }

    async fn finish(&mut self, _output: &mut OutputChannel) {}

    async fn get_state(&mut self) -> RecordBatch {
        RecordBatch::new_empty(Arc::new(Schema::empty()))
    }

    async fn load_state(&mut self, _state: RecordBatch) {}
}

// impl TryFromProto<ProjectionTaskSpec> for ProjectionTask {
//     type Error = DataFusionError;
//
//     fn try_from_proto(session_context: &SessionContext, proto: ProjectionTaskSpec) -> Result<Self, Self::Error> {
//         let input_schema = proto.input_schema.ok_or(internal_datafusion_err!("Schema is required for ProjectionTask"))?;
//         let schema_ref: SchemaRef = input_schema.try_into()?;
//         let expressions = proto.expressions.into_iter()
//             .map(|expr| {
//                 let physical_expr = parse_physical_expr(
//                     &expr.expression.ok_or(internal_datafusion_err!("Expression is required for ProjectionTask"))?,
//                     session_context,
//                     schema_ref.as_ref(),
//                     &RayCodec {},
//                 )?;
//                 Ok((physical_expr, expr.alias))
//             })
//             .collect::<Result<Vec<_>, _>>()?;
//
//         Ok(Self::new(expressions, schema_ref))
//     }
// }
