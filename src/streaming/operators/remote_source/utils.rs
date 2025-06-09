use std::collections::HashSet;
use datafusion::common::DataFusionError;
use crate::streaming::generation::{GenerationInputDetail, GenerationSpec, GenerationStartOffset};

pub fn find_current_generation<'a>(
    stream_ids: &[String],
    generations: &'a [GenerationSpec],
    current_marker: usize,
) -> Result<&'a GenerationSpec, DataFusionError> {
    // Find the latest generation that has a start condition that is valid for the current marker
    let stream_ids_set = stream_ids.iter().cloned().collect::<HashSet<_>>();
    for generation in generations.iter().rev() {
        let valid = generation.start_conditions.iter()
            .filter(|(condition_stream_id, _start_condition)| stream_ids_set.contains(condition_stream_id))
            .all(|(_condition_stream_id, start_condition)| {
                match start_condition {
                    GenerationStartOffset::AnyTimeAfter(minimum_checkpoint) => {
                        minimum_checkpoint <= &current_marker
                    },
                }
            });
        if valid {
            return Ok(generation);
        }
    }
    Err(DataFusionError::Execution("No generation found for the current marker".to_string()))
}

pub fn get_addresses<'a>(
    partitions: &[usize],
    checkpoint: usize,
    stream_ids: &[String],
    input_details: &[GenerationInputDetail],
) -> Result<Vec<(String, String, Vec<usize>)>, DataFusionError> { // returns (stream_id, address, partitions)
    let partitions_set: HashSet<_> = partitions.iter().copied().collect();
    Ok(stream_ids.iter()
        .map(|stream_id| {
            input_details.iter()
                .find(|input_detail| input_detail.stream_id == *stream_id)
                .ok_or_else(|| DataFusionError::Execution(format!("No input detail found for stream ID: {}", stream_id)))
        })
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .map(|input_detail| get_addresses_for_stream(&partitions_set, checkpoint, input_detail))
        .collect::<Result<Vec<_>, DataFusionError>>()?
        // Flatten
        .into_iter()
        .flatten()
        .collect::<Vec<_>>())
}

fn get_addresses_for_stream(
    partitions: &HashSet<usize>,
    checkpoint: usize,
    input_details: &GenerationInputDetail,
) -> Result<Vec<(String, String, Vec<usize>)>, DataFusionError> { // returns (stream_id, address, partitions)
    let mut all_found_partitions = HashSet::new();
    let addresses = input_details.locations.iter()
        // Check that the location range includes the checkpoint
        .filter(|location| location.offset_range.0 <= checkpoint && location.offset_range.1 > checkpoint)
        // Check that the partitions overlap
        .filter_map(|location| {
            let location_partitions_set = location.partitions.iter().copied().collect();
            let overlapping_partitions = partitions.intersection(&location_partitions_set)
                .copied()
                .collect::<Vec<_>>();
            if overlapping_partitions.is_empty() {
                None
            } else {
                all_found_partitions.extend(overlapping_partitions.iter().copied());
                Some((input_details.stream_id.clone(), location.address.clone(), overlapping_partitions))
            }
        })
        .collect::<Vec<_>>();

    // Check that we actually found all the partitions
    let missing_partitions = partitions.iter()
        .filter(|partition| !all_found_partitions.contains(partition))
        .collect::<Vec<_>>();
    if !missing_partitions.is_empty() {
        return Err(DataFusionError::Execution(format!(
            "Missing partitions {:?} for stream {} at checkpoint {}",
            missing_partitions, input_details.stream_id, checkpoint
        )));
    }

    Ok(addresses)
}
