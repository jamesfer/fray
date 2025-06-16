use datafusion::physical_expr::{PhysicalExpr, PhysicalExprRef};
use async_trait::async_trait;
use arrow::array::{AsArray, RecordBatch};
use datafusion::logical_expr::ColumnarValue;
use datafusion::common::{internal_datafusion_err, DataFusionError, ScalarValue};
use std::sync::Arc;
use arrow::datatypes::{Schema, SchemaRef};
use datafusion::prelude::SessionContext;
use crate::streaming::action_stream::StreamItem;
use crate::streaming::operators::task_function::{OutputChannel, OutputChannelL, TaskFunction, TaskState};
use crate::proto::generated::streaming_tasks as proto;
use crate::streaming::operators::serialization::{ProtoSerializer, P};

#[derive(Clone)]
pub struct FilterOperator {
    input_schema: SchemaRef,
    expression: PhysicalExprRef,
}

impl FilterOperator {
    pub fn new(input_schema: SchemaRef, expression: PhysicalExprRef) -> Self {
        Self {
            input_schema,
            expression,
        }
    }

    pub fn into_function(self) -> FilterTask {
        FilterTask::new(self.expression)
    }
}

impl ProtoSerializer for FilterOperator {
    type ProtoType = proto::FilterOperator;
    type SerializerContext<'a> = ();
    type DeserializerContext<'a> = SessionContext;

    fn try_into_proto(self, context: &Self::SerializerContext<'_>) -> Result<Self::ProtoType, DataFusionError> {
        Ok(Self::ProtoType {
            input_schema: Some(self.input_schema.try_into_proto(&())?),
            expression: Some(self.expression.try_into_proto(context)?),
        })
    }

    fn try_from_proto(proto: Self::ProtoType, context: &Self::DeserializerContext<'_>) -> Result<Self, DataFusionError> {
        let input_schema = proto.input_schema
            .ok_or(internal_datafusion_err!("Input schema is required for FilterOperator"))?
            .try_from_proto(&())?;
        Ok(Self {
            expression: proto.expression
                .ok_or(internal_datafusion_err!("Expression is required for FilterOperator"))?
                .try_from_proto(&(context, SchemaRef::as_ref(&input_schema)))?,
            input_schema,
        })
    }
}

pub struct FilterTask {
    expression: PhysicalExprRef,
}

impl FilterTask {
    pub fn new(expression: PhysicalExprRef) -> Self {
        Self {
            expression,
        }
    }
}

#[async_trait]
impl TaskFunction for FilterTask {
    async fn init(&mut self) {}

    async fn poll(&mut self, output: &mut OutputChannelL) -> TaskState {
        unimplemented!()
    }

    async fn process(&mut self, data: RecordBatch, ordinal: usize, output: &mut OutputChannel) -> TaskState {
        let result = self.expression.evaluate(&data).unwrap();
        match result {
            ColumnarValue::Scalar(scalar) => {
                match scalar {
                    ScalarValue::Boolean(boolean) => {
                        if boolean.unwrap_or(false) {
                            output(StreamItem::RecordBatch(data)).await;
                        }
                    },
                    _ => { panic!("Expected boolean scalar value") },
                }
            },
            ColumnarValue::Array(array) => {
                let boolean_array = array.as_boolean();
                let result_batch = arrow::compute::filter_record_batch(&data, &boolean_array).unwrap();
                output(StreamItem::RecordBatch(result_batch)).await;
            },
        }

        TaskState::Continue
    }

    async fn finish(&mut self, output: &mut OutputChannel) {}

    async fn get_state(&mut self) -> RecordBatch {
        RecordBatch::new_empty(Arc::new(Schema::empty()))
    }

    async fn load_state(&mut self, state: RecordBatch) {
        unimplemented!()
    }
}
