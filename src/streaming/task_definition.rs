use bytes::Buf;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::internal_datafusion_err;
use datafusion::error::DataFusionError;
use datafusion::prelude::SessionContext;
use prost::Message;
use crate::streaming::output_manager::OutputSlotPartitioning;
use crate::proto::generated::streaming_tasks as proto;
use crate::streaming::tasks::filter::FilterOperator;
use crate::streaming::tasks::identity::IdentityOperator;
use crate::streaming::tasks::projection::ProjectionOperator;
use crate::streaming::tasks::serialization::{ProtoSerializer, P};
use crate::streaming::tasks::source::SourceOperator;
use crate::streaming::tasks::task_function::TaskFunction;

#[derive(Clone)]
pub struct TaskInputStreamAddress {
    pub address: String,
    pub stream_id: String,
}

impl TryFrom<proto::TaskInputStreamAddress> for TaskInputStreamAddress {
    type Error = DataFusionError;

    fn try_from(proto: proto::TaskInputStreamAddress) -> Result<Self, Self::Error> {
        Ok(Self {
            address: proto.address,
            stream_id: proto.stream_id,
        })
    }
}

impl Into<proto::TaskInputStreamAddress> for TaskInputStreamAddress {
    fn into(self) -> proto::TaskInputStreamAddress {
        proto::TaskInputStreamAddress {
            address: self.address,
            stream_id: self.stream_id,
        }
    }
}

#[derive(Clone)]
pub struct TaskInputStream {
    pub ordinal: usize,
    // pub generations: Vec<TaskInputStreamGeneration>,
    pub addresses: Vec<TaskInputStreamAddress>,
    pub input_schema: SchemaRef,
}

impl TryFrom<proto::TaskInputStream> for TaskInputStream {
    type Error = DataFusionError;

    fn try_from(proto: proto::TaskInputStream) -> Result<Self, Self::Error> {
        let schema = proto.input_schema.ok_or(internal_datafusion_err!("InputSchema is required for TaskInputStream"))?;
        Ok(Self {
            // TODO input schema
            ordinal: proto.ordinal as usize,
            addresses: proto.addresses.into_iter()
                .map(|address| address.try_into())
                .collect::<Result<_, _>>()?,
            input_schema: schema.try_from_proto(&())?,
        })
    }
}

impl Into<proto::TaskInputStream> for TaskInputStream {
    fn into(self) -> proto::TaskInputStream {
        proto::TaskInputStream {
            input_schema: Some(self.input_schema.try_into().unwrap()),
            ordinal: self.ordinal as u64,
            addresses: self.addresses.into_iter().map(|address| address.into()).collect(),
        }
    }
}

#[derive(Clone)]
pub struct TaskInputStreamGeneration {
    // pub addresses: Vec<TaskInputStreamAddress>,
    pub streams: Vec<TaskInputStream>,
    pub transition_after: u64,
    pub partition_range: Vec<usize>,
}

impl TryFrom<proto::TaskInputStreamGeneration> for TaskInputStreamGeneration {
    type Error = DataFusionError;

    fn try_from(proto: proto::TaskInputStreamGeneration) -> Result<Self, Self::Error> {
        Ok(Self {
            streams: proto.streams.into_iter()
                .map(|stream| stream.try_into())
                .collect::<Result<_, _>>()?,
            transition_after: proto.transition_after,
            partition_range: proto.partition_range.iter().map(|x| *x as usize).collect(),
        })
    }
}

impl Into<proto::TaskInputStreamGeneration> for TaskInputStreamGeneration {
    fn into(self) -> proto::TaskInputStreamGeneration {
        proto::TaskInputStreamGeneration {
            streams: self.streams.into_iter().map(|stream| stream.into()).collect(),
            transition_after: self.transition_after,
            partition_range: self.partition_range.iter().map(|x| *x as u64).collect(),
        }
    }
}

#[derive(Clone)]
pub struct TaskInputPhase {
    pub generations: Vec<TaskInputStreamGeneration>,
}

impl TryFrom<proto::TaskInputPhase> for TaskInputPhase {
    type Error = DataFusionError;

    fn try_from(proto: proto::TaskInputPhase) -> Result<Self, Self::Error> {
        Ok(Self {
            generations: proto.generations.into_iter()
                .map(|generation| generation.try_into())
                .collect::<Result<_, _>>()?,
        })
    }
}

impl Into<proto::TaskInputPhase> for TaskInputPhase {
    fn into(self) -> proto::TaskInputPhase {
        proto::TaskInputPhase {
            generations: self.generations.into_iter().map(|generation| generation.into()).collect(),
        }
    }
}

#[derive(Clone)]
pub struct TaskInputDefinition {
    pub phases: Vec<TaskInputPhase>,
}

impl TryFrom<proto::TaskInputDefinition> for TaskInputDefinition {
    type Error = DataFusionError;

    fn try_from(proto: proto::TaskInputDefinition) -> Result<Self, Self::Error> {
        Ok(Self {
            phases: proto.phases.into_iter()
                .map(|phase| phase.try_into())
                .collect::<Result<_, _>>()?,
        })
    }
}

impl Into<proto::TaskInputDefinition> for TaskInputDefinition {
    fn into(self) -> proto::TaskInputDefinition {
        proto::TaskInputDefinition {
            phases: self.phases.into_iter().map(|phase| phase.into()).collect(),
        }
    }
}


// pub struct TaskInputStream {
//     pub addresses: Vec<TaskInputStreamAddress>,
//     pub partition_range: Vec<usize>,
// }
//
// pub struct TaskInputGen {
//     pub transition_after: u64,
//     pub streams: Vec<TaskInputStream>,
// }
//
// pub struct TaskInputDef {
//     pub generations: Vec<TaskInputGeneration>,
//     pub order: TaskInputEvaluationOrder,
// }

#[derive(Clone)]
pub enum TaskSpec {
    Projection(ProjectionOperator),
    Identity(IdentityOperator),
    Source(SourceOperator),
    Filter(FilterOperator),
}

impl TaskSpec {
    pub(crate) fn into_task_function(self) -> Box<dyn TaskFunction + Sync + Send> {
        match self {
            TaskSpec::Projection(projection) => Box::new(projection.into_function()),
            TaskSpec::Source(source) => Box::new(source.into_function()),
            TaskSpec::Identity(identity) => Box::new(identity.into_function()),
            TaskSpec::Filter(filter) => Box::new(filter.into_function()),
        }
    }
}

impl ProtoSerializer for TaskSpec {
    type ProtoType = proto::TaskSpec;
    type SerializerContext<'a> = ();
    type DeserializerContext<'a> = SessionContext;

    fn try_into_proto(self, _context: &Self::SerializerContext<'_>) -> Result<Self::ProtoType, DataFusionError> {
        Ok(Self::ProtoType {
            task: Some(match self {
                Self::Projection(projection) => proto::task_spec::Task::Projection(projection.try_into_proto(&())?),
                Self::Identity(identity) => proto::task_spec::Task::Identity(identity.try_into_proto(&())?),
                Self::Source(source) => proto::task_spec::Task::Source(source.try_into_proto(&())?),
                Self::Filter(filter) => proto::task_spec::Task::Filter(filter.try_into_proto(&())?),
            })
        })
    }

    fn try_from_proto(proto: Self::ProtoType, context: &Self::DeserializerContext<'_>) -> Result<Self, DataFusionError> {
        let task = proto.task.ok_or(internal_datafusion_err!("Task is required for TaskSpec"))?;
        match task {
            proto::task_spec::Task::Projection(projection) => Ok(Self::Projection(projection.try_from_proto(context)?)),
            proto::task_spec::Task::Identity(identity) => Ok(Self::Identity(identity.try_from_proto(&())?)),
            proto::task_spec::Task::Source(source) => Ok(Self::Source(source.try_from_proto(&())?)),
            proto::task_spec::Task::Filter(filter) => Ok(Self::Filter(filter.try_from_proto(context)?)),
        }
    }
}

#[derive(Clone)]
pub struct TaskDefinition {
    pub id: String,
    pub checkpoint_id: String,
    pub inputs: TaskInputDefinition,
    pub spec: TaskSpec,
    pub output_stream_id: String,
    pub output_schema: SchemaRef,
    pub output_partitioning: Option<OutputSlotPartitioning>,
}

impl TaskDefinition {
    pub fn try_decode_from_bytes(bytes: impl Buf, context: &SessionContext) -> Result<TaskDefinition, DataFusionError> {
        let protobuf = proto::TaskDefinition::decode(bytes)
            .map_err(|e| internal_datafusion_err!("Error decoding TaskDefinition: {}", e))?;
        Ok(Self {
            id: protobuf.id,
            checkpoint_id: protobuf.checkpoint_id,
            inputs: protobuf.inputs
                .ok_or(internal_datafusion_err!("TaskInputDefinition is required for TaskDefinition"))?
                .try_into()?,
            spec: protobuf.spec
                .ok_or(internal_datafusion_err!("Function is required for TaskDefinition"))?
                // TODO
                .try_from_proto(context)?,
            output_stream_id: protobuf.output_stream_id,
            output_schema: protobuf.output_schema
                .ok_or(internal_datafusion_err!("OutputSchema is required for TaskDefinition"))?
                .try_from_proto(&())?,
            output_partitioning: protobuf.output_partitioning
                .map(|inner| inner.try_into()).transpose()?,
        })
    }

    fn try_encode_to_proto(self) -> Result<proto::TaskDefinition, DataFusionError> {
        Ok(crate::proto::generated::streaming_tasks::TaskDefinition {
            id: self.id,
            checkpoint_id: self.checkpoint_id,
            inputs: Some(self.inputs.into()),
            spec: Some(self.spec.try_into_proto(&())?),
            output_stream_id: self.output_stream_id,
            output_schema: Some(self.output_schema.try_into_proto(&())?),
            output_partitioning: self.output_partitioning.map(|inner| inner.try_into()).transpose()?,
        })
    }

    pub fn encode_to_bytes(&self) -> Vec<u8> {
        let protobuf: proto::TaskDefinition = self.clone().try_encode_to_proto().unwrap();
        protobuf.encode_to_vec()
    }
}
