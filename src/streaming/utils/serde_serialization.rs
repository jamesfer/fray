use serde::de::{Error, Visitor};

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

pub mod record_batch {
    use std::slice;
    use arrow_array::RecordBatch;
    use serde::{Deserializer, Serializer};
    use serde::de::Error;
    use crate::streaming::utils::serde_serialization::record_batches;

    pub fn serialize<S: Serializer>(record_batches: &RecordBatch, serializer: S) -> Result<S::Ok, S::Error> {
        record_batches::serialize(slice::from_ref(record_batches), serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<RecordBatch, D::Error> {
        let mut vec = record_batches::deserialize(deserializer)?;
        match vec.len() {
            1 => Ok(vec.pop().unwrap()),
            0 => Err(Error::custom("Cannot deserialize a record batch from an empty stream")),
            _ => Err(Error::custom("Found more than one record batch when deserializing the stream")),
        }
    }
}

pub mod record_batches {
    use arrow::array::RecordBatch;
    use arrow::ipc::reader::StreamReader;
    use arrow::ipc::writer::StreamWriter;
    use serde::de::Error as DeError;
    use serde::ser::Error as SerError;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(record_batches: &[RecordBatch], serializer: S) -> Result<S::Ok, S::Error> {
        // Create a stream writer with the schema from the first batch
        match record_batches.first() {
            None => {
                serializer.serialize_bytes(&[])
            },
            Some(record_batch) => {
                let mut buffer = Vec::new();
                let mut writer = StreamWriter::try_new(&mut buffer, record_batch.schema_ref())
                    .map_err(|arrow_error| S::Error::custom(arrow_error))?;

                // Write each batch to the buffer
                for batch in record_batches {
                    writer.write(batch)
                        .map_err(|arrow_error| S::Error::custom(arrow_error))?;
                }

                // Finish writing to flush any remaining data
                writer.finish()
                    .map_err(|arrow_error| S::Error::custom(arrow_error))?;

                serializer.serialize_bytes(&buffer)
            },
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<RecordBatch>, D::Error> {
        let bytes: &[u8] = Deserialize::deserialize(deserializer)?;
        let mut reader = StreamReader::try_new(bytes, None)
            .map_err(|arrow_error| D::Error::custom(arrow_error))?;

        let mut record_batches = Vec::new();
        while let Some(result) = reader.next() {
            match result {
                Ok(record_batch) => record_batches.push(record_batch),
                Err(error) => {
                    return Err(D::Error::custom(format!("Failed to read record batch: {}", error)));
                }
            }
        }

        Ok(record_batches)
    }

    // struct RecordBatchesWrapper(pub Vec<RecordBatch>);
    //
    // impl Serialize for RecordBatchesWrapper {
    //     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    //     where S: Serializer
    //     {
    //         // Create a stream writer with the schema from the first batch
    //         match self.0.first() {
    //             None => {
    //                 serializer.serialize_bytes(&[])
    //             },
    //             Some(record_batch) => {
    //                 let mut buffer = Vec::new();
    //                 let mut writer = StreamWriter::try_new(&mut buffer, record_batch.schema_ref())
    //                     .map_err(|arrow_error| S::Error::custom(arrow_error))?;
    //
    //                 // Write each batch to the buffer
    //                 for batch in &self.0 {
    //                     writer.write(batch)
    //                         .map_err(|arrow_error| S::Error::custom(arrow_error))?;
    //                 }
    //
    //                 // Finish writing to flush any remaining data
    //                 writer.finish()
    //                         .map_err(|arrow_error| S::Error::custom(arrow_error))?;
    //
    //                 serializer.serialize_bytes(&buffer)
    //             },
    //         }
    //     }
    // }
    //
    // impl <'de> Deserialize<'de> for RecordBatchesWrapper {
    //     fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    //     where D: Deserializer<'de>,
    //     {
    //         let bytes: &[u8] = Deserialize::deserialize(deserializer)?;
    //         let mut reader = StreamReader::try_new(bytes, None)
    //             .map_err(|arrow_error| D::Error::custom(arrow_error))?;
    //
    //         let mut record_batches = Vec::new();
    //         while let Some(result) = reader.next() {
    //             match result {
    //                 Ok(record_batch) => record_batches.push(record_batch),
    //                 Err(error) => {
    //                     return Err(D::Error::custom(format!("Failed to read record batch: {}", error)));
    //                 }
    //             }
    //         }
    //
    //         Ok(RecordBatchesWrapper(record_batches))
    //     }
    // }
}

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
