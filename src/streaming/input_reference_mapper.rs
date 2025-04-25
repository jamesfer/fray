use std::collections::HashMap;
use crate::streaming::task_definition::TaskInputStreamT;

#[derive(Clone)]
pub struct InputReference {
    pub address: String,
    pub partition_range: Vec<usize>,
}

pub struct InputReferenceMapper {
    streams: HashMap<String, Vec<String>>,
}

impl InputReferenceMapper {
    pub fn new(streams: Vec<TaskInputStreamT>) -> Self {
        Self {
            streams: streams.into_iter()
                .map(|stream| {
                    (stream.stream_id, stream.address)
                })
                .collect(),
        }
    }

   pub fn get_addresses_for(&self, stream_id: &str, partition_range: &[usize]) -> Option<Vec<InputReference>> {
       todo!()
   }
}
