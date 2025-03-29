#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TaskInputStreamAddress {
    #[prost(string, tag = "1")]
    pub address: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub stream_id: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TaskInputStream {
    #[prost(uint64, tag = "1")]
    pub ordinal: u64,
    #[prost(message, repeated, tag = "2")]
    pub addresses: ::prost::alloc::vec::Vec<TaskInputStreamAddress>,
    #[prost(message, optional, tag = "3")]
    pub input_schema: ::core::option::Option<::datafusion_proto::protobuf::Schema>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TaskInputStreamGeneration {
    #[prost(message, repeated, tag = "1")]
    pub streams: ::prost::alloc::vec::Vec<TaskInputStream>,
    #[prost(uint64, tag = "2")]
    pub transition_after: u64,
    #[prost(uint64, repeated, tag = "3")]
    pub partition_range: ::prost::alloc::vec::Vec<u64>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TaskInputPhase {
    #[prost(message, repeated, tag = "1")]
    pub generations: ::prost::alloc::vec::Vec<TaskInputStreamGeneration>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TaskInputDefinition {
    #[prost(message, repeated, tag = "2")]
    pub phases: ::prost::alloc::vec::Vec<TaskInputPhase>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RoundRobinPartitioning {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct HashPartitioning {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OutputPartitionStrategy {
    #[prost(oneof = "output_partition_strategy::Strategy", tags = "1, 2")]
    pub strategy: ::core::option::Option<output_partition_strategy::Strategy>,
}
/// Nested message and enum types in `OutputPartitionStrategy`.
pub mod output_partition_strategy {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Strategy {
        #[prost(message, tag = "1")]
        RoundRobin(super::RoundRobinPartitioning),
        #[prost(message, tag = "2")]
        Hash(super::HashPartitioning),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OutputSlotPartitioning {
    #[prost(message, optional, tag = "1")]
    pub strategy: ::core::option::Option<OutputPartitionStrategy>,
    #[prost(uint64, tag = "2")]
    pub total_partitions: u64,
    #[prost(uint64, repeated, tag = "3")]
    pub partition_range: ::prost::alloc::vec::Vec<u64>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TaskDefinition {
    #[prost(string, tag = "1")]
    pub id: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub checkpoint_id: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "3")]
    pub inputs: ::core::option::Option<TaskInputDefinition>,
    #[prost(message, optional, tag = "4")]
    pub spec: ::core::option::Option<TaskSpec>,
    #[prost(string, tag = "5")]
    pub output_stream_id: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "6")]
    pub output_schema: ::core::option::Option<::datafusion_proto::protobuf::Schema>,
    #[prost(message, optional, tag = "7")]
    pub output_partitioning: ::core::option::Option<OutputSlotPartitioning>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ProjectionExpression {
    #[prost(message, optional, tag = "1")]
    pub expression: ::core::option::Option<
        ::datafusion_proto::protobuf::PhysicalExprNode,
    >,
    #[prost(string, tag = "2")]
    pub alias: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ProjectionOperator {
    /// Needed to decode the expressions
    #[prost(message, optional, tag = "1")]
    pub input_schema: ::core::option::Option<::datafusion_proto::protobuf::Schema>,
    #[prost(message, repeated, tag = "2")]
    pub expressions: ::prost::alloc::vec::Vec<ProjectionExpression>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IdentityOperator {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SourceOperator {
    #[prost(bytes = "vec", repeated, tag = "1")]
    pub record_batches: ::prost::alloc::vec::Vec<::prost::alloc::vec::Vec<u8>>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FilterOperator {
    #[prost(message, optional, tag = "1")]
    pub input_schema: ::core::option::Option<::datafusion_proto::protobuf::Schema>,
    #[prost(message, optional, tag = "2")]
    pub expression: ::core::option::Option<
        ::datafusion_proto::protobuf::PhysicalExprNode,
    >,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UnionOperator {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TaskSpec {
    #[prost(oneof = "task_spec::Task", tags = "1, 2, 3, 4, 5")]
    pub task: ::core::option::Option<task_spec::Task>,
}
/// Nested message and enum types in `TaskSpec`.
pub mod task_spec {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Task {
        #[prost(message, tag = "1")]
        Projection(super::ProjectionOperator),
        #[prost(message, tag = "2")]
        Identity(super::IdentityOperator),
        #[prost(message, tag = "3")]
        Source(super::SourceOperator),
        #[prost(message, tag = "4")]
        Filter(super::FilterOperator),
        #[prost(message, tag = "5")]
        Union(super::UnionOperator),
    }
}
