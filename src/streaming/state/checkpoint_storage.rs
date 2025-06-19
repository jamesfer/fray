use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use datafusion::common::{internal_datafusion_err, DataFusionError, Result};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::streaming::partitioning::PartitionRange;
use crate::streaming::state::file_system::FileSystemStorage;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionInfo {
    pub partition_range: PartitionRange,
    pub timestamp: u64,
    pub checkpoint_dir: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointEntry {
    pub id: String,
    pub partitions: Vec<PartitionInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatestCheckpoints {
    pub completed_checkpoints: Vec<CheckpointEntry>,
}

pub struct FileSystemStateStorage {
    storage: Arc<dyn FileSystemStorage + Send + Sync>,
    root_dir: PathBuf,
}

impl FileSystemStateStorage {
    pub fn new<P: AsRef<Path>>(
        storage: Arc<dyn FileSystemStorage + Send + Sync>,
        root_dir: P,
    ) -> Self {
        Self {
            storage,
            root_dir: root_dir.as_ref().to_path_buf(),
        }
    }

    pub fn storage(&self) -> &Arc<dyn FileSystemStorage + Send + Sync> {
        &self.storage
    }

    pub async fn start_checkpoint(&self, operator_id: &str) -> Result<PathBuf> {
        let checkpoint_id = Uuid::new_v4().to_string();
        let checkpoint_path = self.get_checkpoint_path(operator_id, &checkpoint_id);
        
        self.storage.mkdir_all(&checkpoint_path).await?;
        
        Ok(checkpoint_path)
    }

    pub async fn complete_checkpoint(
        &self,
        operator_id: &str,
        checkpoint_id: &str,
        checkpoint_dir: &Path,
        partition_range: PartitionRange,
    ) -> Result<()> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| DataFusionError::Internal(format!("Failed to get current time: {}", e)))?
            .as_millis() as u64;

        let partition_info = PartitionInfo {
            partition_range,
            timestamp,
            checkpoint_dir: checkpoint_dir.to_string_lossy().to_string(),
        };

        let latest_path = self.get_latest_json_path(operator_id);
        let mut latest = self.read_latest_json(&latest_path).await?;

        if let Some(existing_entry) = latest
            .completed_checkpoints
            .iter_mut()
            .find(|entry| entry.id == checkpoint_id)
        {
            existing_entry.partitions.push(partition_info);
        } else {
            let new_entry = CheckpointEntry {
                id: checkpoint_id.to_string(),
                partitions: vec![partition_info],
            };
            latest.completed_checkpoints.push(new_entry);
        }

        self.write_latest_json(&latest_path, &latest).await?;

        Ok(())
    }

    pub async fn get_operator_checkpoints(
        &self,
        operator_id: &str,
        partition_range: &PartitionRange,
        checkpoint_id: &str,
    ) -> Result<Vec<(String, PartitionRange)>> {
        let latest_path = self.get_latest_json_path(operator_id);
        let latest = self.read_latest_json(&latest_path).await?;

        println!("Read latest file: {:?}", latest);

        Ok(latest
            .completed_checkpoints
            .into_iter()
            // Only consider checkpoints that match the given id
            .filter(|checkpoint_entry| &checkpoint_entry.id == checkpoint_id)
            .find_map(|checkpoint_entry| {
                let partition_ranges = checkpoint_entry.partitions
                    .iter()
                    .map(|partition_info| partition_info.partition_range.clone())
                    .collect::<Vec<_>>();
                let intersecting_partitions = Self::get_non_overlapping_partitions(partition_range, &partition_ranges)?;
                let partition_infos = intersecting_partitions.into_iter()
                    .map(|overlap| (
                        checkpoint_entry.partitions[overlap.index].checkpoint_dir.clone(),
                        overlap.selected_partition_range,
                    ))
                    .collect::<Vec<_>>();
                Some(partition_infos)
            })
            .ok_or(internal_datafusion_err!(
                "No viable checkpoints found for operator {} in range {:?}",
                operator_id,
                partition_range
            ))?)
    }

    pub async fn get_latest_operator_checkpoints(
        &self,
        operator_id: &str,
        partition_range: &PartitionRange,
    ) -> Result<(String, Vec<(String, PartitionRange)>)> {
        let latest_path = self.get_latest_json_path(operator_id);
        let latest = self.read_latest_json(&latest_path).await?;

        Ok(latest
            .completed_checkpoints
            .into_iter()
            .find_map(|checkpoint_entry| {
                let partition_ranges = checkpoint_entry.partitions
                    .iter()
                    .map(|partition_info| partition_info.partition_range.clone())
                    .collect::<Vec<_>>();
                let intersecting_partitions = Self::get_non_overlapping_partitions(partition_range, &partition_ranges)?;
                let partition_infos = intersecting_partitions.into_iter()
                    .map(|overlap| (
                        checkpoint_entry.partitions[overlap.index].checkpoint_dir.clone(),
                        overlap.selected_partition_range,
                    ))
                    .collect::<Vec<_>>();
                Some((checkpoint_entry.id, partition_infos))
            })
            .ok_or(internal_datafusion_err!(
                "No viable checkpoints found for operator {} in range {:?}",
                operator_id,
                partition_range
            ))?)
    }

    fn get_checkpoint_path(&self, operator_id: &str, checkpoint_id: &str) -> PathBuf {
        self.root_dir
            .join("operators")
            .join(operator_id)
            .join("checkpoints")
            .join(checkpoint_id)
    }

    fn get_latest_json_path(&self, operator_id: &str) -> PathBuf {
        self.root_dir
            .join("operators")
            .join(operator_id)
            .join("latest.json")
    }

    async fn read_latest_json(&self, path: &Path) -> Result<LatestCheckpoints> {
        match self.storage.read_file(path).await {
            Ok(content) => {
                let content_str = String::from_utf8(content).map_err(|e| {
                    DataFusionError::Internal(format!("Failed to parse file as UTF-8: {}", e))
                })?;
                
                serde_json::from_str(&content_str).map_err(|e| {
                    DataFusionError::Internal(format!("Failed to parse JSON: {}", e))
                })
            }
            Err(_) => {
                Ok(LatestCheckpoints {
                    completed_checkpoints: Vec::new(),
                })
            }
        }
    }

    async fn write_latest_json(&self, path: &Path, latest: &LatestCheckpoints) -> Result<()> {
        let json_content = serde_json::to_string_pretty(latest).map_err(|e| {
            DataFusionError::Internal(format!("Failed to serialize JSON: {}", e))
        })?;

        if let Some(parent) = path.parent() {
            self.storage.mkdir_all(parent).await?;
        }

        self.storage
            .write_file(path, json_content.as_bytes())
            .await?;
        println!("Wrote latest json to {:?}, {:?}", path, latest);

        Ok(())
    }

    fn get_non_overlapping_partitions(
        search_range: &PartitionRange,
        available_partitions: &[PartitionRange],
    ) -> Option<Vec<OverlappingPartition>> {
        // Easy implementation, not quite optimal, but good enough for now.
        
        // All partition ranges will be converted to the same partition count for comparison
        let target_partitions = available_partitions.iter()
            .map(|partition_range| partition_range.partitions())
            .chain([search_range.partitions()])
            .max()?;
        let search_start = search_range.start() * target_partitions / search_range.partitions();
        let search_end = search_range.end() * target_partitions / search_range.partitions();
        
        // Normalize all available partitions to the target partition count
        // (original index, normalised start, normalised end)
        let mut normalized_partitions: Vec<(usize, usize, usize)> = available_partitions
            .iter()
            .enumerate()
            .map(|(index, partition_range)| {
                let normalized_start = partition_range.start() * target_partitions / partition_range.partitions();
                let normalized_end = partition_range.end() * target_partitions / partition_range.partitions();
                (index, normalized_start, normalized_end)
            })
            .filter(|(_index, start, end)| {
                // Check if the partition overlaps with the search range
                *start < search_end && *end > search_start
            })
            .collect();
        
        // Sort by start position
        normalized_partitions.sort_by_key(|(start, _, _)| *start);

        let mut current_search_start = search_start;
        let mut found_partitions = Vec::new();
        for (index, start, end) in normalized_partitions {
            if end > current_search_start {
                // If the partition starts after the current search start, we have a gap
                if start > current_search_start {
                    // No overlapping partitions found
                    return None;
                }

                // Update the current search start to the end of this partition
                let overlap_end = search_end.min(end);
                found_partitions.push(OverlappingPartition {
                    index,
                    selected_partition_range: PartitionRange::new(
                        current_search_start,
                        overlap_end,
                        target_partitions,
                    ),
                });

                current_search_start = overlap_end;
                if current_search_start >= search_end {
                    // We have finished searching
                    return Some(found_partitions);
                }
            }
        }

        // If we reach here, it means we didn't cover the entire search range
        assert!(current_search_start < search_end, "Search algorithm finished without returning the success case");
        None
    }
}

#[derive(Debug, Clone, PartialEq)]
struct OverlappingPartition {
    index: usize,
    selected_partition_range: PartitionRange,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::streaming::state::file_system::TempdirFileSystemStorage;
    use std::sync::Arc;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_start_checkpoint() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let storage = Arc::new(TempdirFileSystemStorage::from_tempdir(temp_dir));
        let checkpoint_storage = FileSystemStateStorage::new(storage, "test");

        let checkpoint_path = checkpoint_storage.start_checkpoint("operator1").await?;
        
        assert!(checkpoint_path.to_string_lossy().contains("operators/operator1/checkpoints/"));
        
        Ok(())
    }

    #[tokio::test]
    async fn test_complete_checkpoint() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let storage = Arc::new(TempdirFileSystemStorage::from_tempdir(temp_dir));
        let checkpoint_storage = FileSystemStateStorage::new(storage, "test");

        let checkpoint_path = checkpoint_storage.start_checkpoint("operator1").await?;
        let partition_range = PartitionRange::new(0, 1, 10);
        
        checkpoint_storage.complete_checkpoint("operator1", "chk1", &checkpoint_path, partition_range).await?;

        let latest_path = checkpoint_storage.get_latest_json_path("operator1");
        let latest = checkpoint_storage.read_latest_json(&latest_path).await?;
        
        assert_eq!(latest.completed_checkpoints.len(), 1);
        assert_eq!(latest.completed_checkpoints[0].partitions.len(), 1);
        assert_eq!(latest.completed_checkpoints[0].partitions[0].partition_range.start(), 0);
        assert_eq!(latest.completed_checkpoints[0].partitions[0].partition_range.end(), 1);
        
        Ok(())
    }

    #[test]
    fn test_get_non_overlapping_partitions() {
        // Test case 1: Perfect coverage with non-overlapping partitions
        let search_range = PartitionRange::new(0, 4, 4); // Covers partitions 0,1,2,3 out of 4
        let available_partitions = vec![
            PartitionRange::new(0, 2, 4), // Covers 0,1
            PartitionRange::new(2, 4, 4), // Covers 2,3
        ];
        
        let result = FileSystemStateStorage::get_non_overlapping_partitions(&search_range, &available_partitions);
        assert_eq!(result, Some(vec![
            OverlappingPartition {
                index: 0,
                selected_partition_range: PartitionRange::new(0, 2, 4),
            },
            OverlappingPartition {
                index: 1,
                selected_partition_range: PartitionRange::new(2, 4, 4),
            },
        ]));

        // Test case 2: No coverage possible due to gap
        let search_range = PartitionRange::new(0, 4, 4);
        let available_partitions = vec![
            PartitionRange::new(0, 1, 4), // Covers 0 only
            PartitionRange::new(3, 4, 4), // Covers 3 only (gap at 1,2)
        ];
        
        let result = FileSystemStateStorage::get_non_overlapping_partitions(&search_range, &available_partitions);
        assert!(result.is_none());
        
        // Test case 3: Different partition counts (normalised comparison)
        let search_range = PartitionRange::new(0, 1, 2); // Half of 2 partitions = equivalent to 0-5 of 10
        let available_partitions = vec![
            PartitionRange::new(0, 5, 10), // First half in 10-partition system
        ];
        
        let result = FileSystemStateStorage::get_non_overlapping_partitions(&search_range, &available_partitions);
        assert_eq!(result, Some(vec![
            OverlappingPartition {
                index: 0,
                selected_partition_range: PartitionRange::new(0, 5, 10),
            },
        ]));

        // Test case 4: Search range has fewer base partitions, and available partitions don't
        // divide evenly
        let search_range = PartitionRange::new(0, 1, 2); // 2 partitions total
        let available_partitions = vec![
            PartitionRange::new(0, 3, 8), // 8 partitions total, covers first half
            PartitionRange::new(3, 4, 8), // 8 partitions total, covers second half
        ];
        
        let result = FileSystemStateStorage::get_non_overlapping_partitions(&search_range, &available_partitions);
        assert_eq!(result, Some(vec![
            OverlappingPartition {
                index: 0,
                selected_partition_range: PartitionRange::new(0, 3, 8),
            },
            OverlappingPartition {
                index: 1,
                selected_partition_range: PartitionRange::new(3, 4, 8),
            },
        ]));

        // Test case 5: Ranges that extend outside search range and overlap with each other
        let search_range = PartitionRange::new(2, 6, 8); // 2 partitions total
        let available_partitions = vec![
            PartitionRange::new(3, 8, 16),
            PartitionRange::new(0, 3, 16), // Should be excluded
            PartitionRange::new(6, 16, 16),
            PartitionRange::new(7, 16, 16),
        ];

        let result = FileSystemStateStorage::get_non_overlapping_partitions(&search_range, &available_partitions);
        assert_eq!(result, Some(vec![
            OverlappingPartition {
                index: 0,
                selected_partition_range: PartitionRange::new(4, 8, 16),
            },
            OverlappingPartition {
                index: 2,
                selected_partition_range: PartitionRange::new(8, 12, 16),
            },
        ]))
    }
}
