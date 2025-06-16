use std::collections::hash_map::Entry;
use std::collections::HashMap;
use datafusion::arrow;
use datafusion::arrow::array::{Array, Scalar};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::record_batch::RecordBatch;
use tokio::sync::RwLock;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CheckpointIdentifier {
    pub checkpoint_id: String,
    pub marker_index: u64,
}

#[derive(Debug, Clone)]
struct Checkpoint {
    written_partitions: Vec<usize>,
    data: RecordBatch,
}

pub struct CheckpointStorageManager {
    stored_checkpoints: RwLock<HashMap<CheckpointIdentifier, Checkpoint>>,
}

impl CheckpointStorageManager {
    pub fn new() -> Self {
        Self {
            stored_checkpoints: RwLock::new(HashMap::new()),
        }
    }

    pub async fn write_checkpoint_state(&self, checkpoint_id: String, partitions: &[usize], marker_index: u64, state: RecordBatch) -> Result<(), String> {
        // Hacky quick exit for tasks with no state
        if state.num_columns() == 0 && state.num_rows() == 0 {
            return Ok(());
        }

        let id = CheckpointIdentifier { checkpoint_id, marker_index };
        match self.stored_checkpoints.write().await.entry(id) {
            Entry::Occupied(mut occupied) => {
                let checkpoint = occupied.get_mut();
                let schema = checkpoint.data.schema_ref();
                if schema != state.schema_ref() {
                    return Err("Schema did not match existing checkpoint schema".to_string());
                }

                checkpoint.data = arrow::compute::concat_batches(schema, vec![checkpoint.data.clone(), state].iter())
                    .map_err(|err| format!("Encountered ArrowError when concatenating state batches: {:?}", err))?;
                checkpoint.written_partitions.extend_from_slice(partitions);
            },
            Entry::Vacant(slot) => {
                if state.schema_ref().fields.is_empty() || state.schema_ref().field(0).data_type() != &DataType::UInt64 {
                    return Err(format!("Invalid checkpoint schema. First column must be a hash of type Int64, found {}", state.schema_ref().field(0).data_type()));
                }
                slot.insert(Checkpoint {
                    written_partitions: partitions.to_vec(),
                    data: state,
                });
            },
        }
        Ok(())
    }

    // TODO need to know the total number of partitions. We assume it is 10 for now
    pub async fn get_checkpoint_state(&self, checkpoint_id: String, partitions: &[usize], marker_index: u64) -> Option<RecordBatch> {
        const MAX_PARTITIONS: usize = 10;
        let id = CheckpointIdentifier { checkpoint_id, marker_index };
        let stored_checkpoints = self.stored_checkpoints.read().await;
        let x = stored_checkpoints.get(&id);

        println!("Stored checkpoint markers: {:?}", stored_checkpoints.keys().collect::<Vec<_>>());

        x.map(|checkpoint| {
            if !partitions.iter().all(|partition| checkpoint.written_partitions.contains(partition)) {
                panic!("Not all partitions have been written to checkpoint");
            }

            let partition_numbers = checkpoint.data.column(0).as_any()
                .downcast_ref::<arrow::array::UInt64Array>()
                .unwrap();
            let mut match_partition_builder = arrow::array::BooleanBuilder::with_capacity(partition_numbers.len());
            match_partition_builder.append_slice(&partition_numbers.iter()
                .map(|partition| partitions.contains(&(partition.unwrap() as usize % MAX_PARTITIONS)))
                .collect::<Vec<_>>());
            let match_partition_array = match_partition_builder.finish();

            let matched_state = arrow::compute::filter_record_batch(&checkpoint.data, &match_partition_array).unwrap();
            matched_state
        })
    }
}
