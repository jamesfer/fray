#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StreamingFlightTicketData {
    /// output stream id
    #[prost(string, tag = "1")]
    pub stream_id: ::prost::alloc::string::String,
    /// partition id of the stream
    #[prost(uint64, tag = "2")]
    pub partition: u64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Marker {
    #[prost(uint64, tag = "1")]
    pub checkpoint_number: u64,
}
