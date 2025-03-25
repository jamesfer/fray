// mod schema {
//     use arrow::datatypes::SchemaRef;
//     use prost::Message;
//     use serde::de::{Error, Visitor};
//     use serde::{Deserializer, Serializer};
//     use std::fmt::Formatter;
//
//     pub fn serialize<S: Serializer>(schema: &SchemaRef, serializer: S) -> Result<S::Ok, S::Error> {
//         // The conversion is infallible
//         let proto_schema = datafusion_proto::protobuf::Schema::try_from(schema).unwrap();
//         let bytes = proto_schema.encode_to_vec();
//         serializer.serialize_bytes(&bytes)
//     }
//
//     pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<SchemaRef, D::Error> {
//         let proto_schema = deserializer.deserialize_bytes(V)?;
//         proto_schema.try_into()
//             .map_err(|e| Error::custom(format!("failed to convert schema: {e}")))
//     }
//
//     struct V;
//
//     impl <'de> Visitor<'de> for V {
//         type Value = datafusion_proto::protobuf::Schema;
//
//         fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
//             formatter.write_str("a byte array")
//         }
//
//         fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
//         where
//             E: Error
//         {
//             datafusion_proto::protobuf::Schema::decode(v)
//                 .map_err(|e| E::custom(format!("failed to decode schema: {e}")))
//         }
//     }
// }
//
// mod physical_expr {
//     use arrow::datatypes::SchemaRef;
//     use prost::Message;
//     use serde::de::{Error, Visitor};
//     use serde::{Deserializer, Serializer};
//     use std::fmt::Formatter;
//     use datafusion::physical_expr::PhysicalExprRef;
//
//     pub fn serialize<S: Serializer>(expr: &PhysicalExprRef, serializer: S) -> Result<S::Ok, S::Error> {
//         // The conversion is infallible
//         expr.
//         let proto_schema = datafusion_proto::protobuf::PhysicalExprNode::try_from()
//         let bytes = proto_schema.encode_to_vec();
//         serializer.serialize_bytes(&bytes)
//     }
//
//     pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<SchemaRef, D::Error> {
//         let proto_schema = deserializer.deserialize_bytes(V)?;
//         proto_schema.try_into()
//             .map_err(|e| Error::custom(format!("failed to convert schema: {e}")))
//     }
//
//     struct V;
//
//     impl <'de> Visitor<'de> for V {
//         type Value = datafusion_proto::protobuf::Schema;
//
//         fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
//             formatter.write_str("a byte array")
//         }
//
//         fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
//         where
//             E: Error
//         {
//             datafusion_proto::protobuf::Schema::decode(v)
//                 .map_err(|e| E::custom(format!("failed to decode schema: {e}")))
//         }
//     }
// }
