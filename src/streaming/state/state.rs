use std::ffi::OsString;
use std::mem;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use futures::{StreamExt, TryStreamExt};
use async_trait::async_trait;
use futures_util::TryFutureExt;
use rocksdb::checkpoint::Checkpoint;
use tokio::fs::DirEntry;
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReadDirStream;
use datafusion::common::{internal_datafusion_err, DataFusionError, HashMap};
use crate::streaming::partitioning::PartitionRange;

// struct HashMapState<K, V> {
//     file_system_storage: Arc<FileSystemStorage>,
//     view: HashMap<K, V>,
//     snapshots: Vec<(usize, HashMap<K, V>)>,
// }
//
// impl <K, V> HashMapState<K, V> {
//     pub fn new(
//         file_system_storage: Arc<FileSystemStorage>,
//     ) -> Self {
//
//     }
//
//     pub fn set(&mut self, key: K, value: V) {
//         self.view.insert(key, value);
//     }
//
//     pub fn get(&self, key: &K) -> Option<&V> {
//         self.view.get(key)
//     }
//
//     // Creates an in memory checkpoint with the current state instantaneously, then starts a
//     // background task to persist the checkpoint to the underlying storage.
//     pub fn create_async_checkpoint(&mut self, id: usize) {
//         let snapshot = self.view.clone();
//         self.snapshots.push((id, snapshot));
//         tokio::spawn({
//             let file_system_storage = self.file_system_storage.clone();
//             async move {
//                 file_system_storage.write_file()
//             }
//         });
//     }
//
//     pub fn get_checkpoints(&self) {
//
//     }
//
//     pub async fn move_to_checkpoint(&mut self, id: usize) -> Result<(), DataFusionError> {
//
//     }
// }
//
//
//

#[derive(Debug, Clone)]
struct FileSystemEntry {
    name: OsString,
    directory: bool,
}

// TODO consider replacing with object_store::local::LocalFileSystem;
#[async_trait]
pub trait FileSystemStorage {
    async fn write_file(&self, path: &Path, contents: &[u8]) -> Result<(), DataFusionError>;
    async fn read_file(&self, path: &Path) -> Result<Vec<u8>, DataFusionError>;
    async fn list_files(&self, path: &Path) -> Result<Vec<FileSystemEntry>, DataFusionError>;
    async fn copy(&self, src: &Path, dst: &Path) -> Result<(), DataFusionError>;
    async fn remove_dir_all(&self, path: &Path) -> Result<(), DataFusionError>;
    async fn mkdir_all(&self, path: &Path) -> Result<(), DataFusionError>;
    fn get_physical_path(&self, path: &Path) -> Result<PathBuf, DataFusionError>;
}

pub struct PrefixedLocalFileSystemStorage {
    dir: OsString,
}

impl PrefixedLocalFileSystemStorage {
    pub fn new(dir: impl Into<OsString>) -> Self {
        Self { dir: dir.into() }
    }

    fn prefixed_path(&self, path: impl AsRef<Path>) -> PathBuf {
        Path::new(&self.dir).join(&path)
    }
}

#[async_trait]
impl FileSystemStorage for PrefixedLocalFileSystemStorage {
    async fn write_file(&self, path: &Path, contents: &[u8]) -> Result<(), DataFusionError> {
        tokio::fs::write(self.prefixed_path(path), contents).await
            .map_err(|e| DataFusionError::Execution(format!("Failed to write file: {}", e)))
    }

    async fn read_file(&self, path: &Path) -> Result<Vec<u8>, DataFusionError> {
        tokio::fs::read(self.prefixed_path(path)).await
            .map_err(|e| DataFusionError::Execution(format!("Failed to read file: {}", e)))
    }

    async fn list_files(&self, path: &Path) -> Result<Vec<FileSystemEntry>, DataFusionError> {
        let stream = ReadDirStream::new(tokio::fs::read_dir(self.prefixed_path(path)).await
            .map_err(|e| DataFusionError::Execution(format!("Failed to read dir: {}", e)))?);

        let stream = stream
            .map_err(|err| internal_datafusion_err!("Error when reading directory: {}", err))
            .flat_map(|result| futures::stream::once(async {
                match result {
                    Err(err) => Err(err),
                    Ok(entry) => {
                        Ok(FileSystemEntry {
                            name: entry.file_name(),
                            directory: entry.file_type().await.map(|ft| ft.is_dir()).unwrap_or_else(|_| false),
                        })
                    }
                }
            }))
            .try_collect::<Vec<_>>().await?;

        Ok(stream)
    }

    async fn copy(&self, src: &Path, dst: &Path) -> Result<(), DataFusionError> {
        copy(Path::new(&self.dir), src, dst).await
    }

    async fn remove_dir_all(&self, path: &Path) -> Result<(), DataFusionError> {
        tokio::fs::remove_dir_all(self.prefixed_path(path)).await
            .map_err(|e| DataFusionError::Execution(format!("Failed to remove directory: {}", e)))
    }

    async fn mkdir_all(&self, path: &Path) -> Result<(), DataFusionError> {
        tokio::fs::create_dir_all(self.prefixed_path(path)).await
            .map_err(|e| DataFusionError::Execution(format!("Failed to create directory: {}", e)))
    }

    fn get_physical_path(&self, path: &Path) -> Result<PathBuf, DataFusionError> {
        Ok(self.prefixed_path(path))
    }
}

async fn copy(root_dir: &Path, src: &Path, dst: &Path) -> Result<(), DataFusionError> {
    // Copy individual files
    let absolute_src_path = Path::new(&root_dir).join(&src);
    let absolute_dst_path = Path::new(&root_dir).join(&dst);
    if !absolute_src_path.is_dir() {
        tokio::fs::copy(absolute_src_path, absolute_dst_path).await
            .map_err(|e| DataFusionError::Execution(format!("Failed to copy file: {}", e)))?;
        return Ok(());
    }

    // Copy directories recursively
    tokio::fs::create_dir_all(absolute_dst_path).await
        .map_err(|e| DataFusionError::Execution(format!("Failed to create directory: {}", e)))?;
    let mut entries = tokio::fs::read_dir(absolute_src_path).await
        .map_err(|e| DataFusionError::Execution(format!("Failed to read directory: {}", e)))?;
    while let Some(entry) = entries.next_entry().await
        .map_err(|e| DataFusionError::Execution(format!("Failed to read directory entry: {}", e)))? {
        let file_name = entry.file_name();
        let src_path = Path::new(src).join(&file_name);
        let dst_path = Path::new(dst).join(&file_name);
        Box::pin(copy(&root_dir, &src_path, &dst_path)).await?;
    }
    Ok(())
}

pub struct TempdirFileSystemStorage {
    dir: tempfile::TempDir,
    inner: PrefixedLocalFileSystemStorage,
}

impl TempdirFileSystemStorage {
    pub fn from_tempdir(dir: tempfile::TempDir) -> Self {
        Self {
            inner: PrefixedLocalFileSystemStorage::new(dir.path()),
            dir,
        }
    }
}

#[async_trait]
impl FileSystemStorage for TempdirFileSystemStorage {
    async fn write_file(&self, path: &Path, contents: &[u8]) -> Result<(), DataFusionError> {
        self.inner.write_file(path, contents).await
    }

    async fn read_file(&self, path: &Path) -> Result<Vec<u8>, DataFusionError> {
        self.inner.read_file(path).await
    }

    async fn list_files(&self, path: &Path) -> Result<Vec<FileSystemEntry>, DataFusionError> {
        self.inner.list_files(path).await
    }

    async fn copy(&self, src: &Path, dst: &Path) -> Result<(), DataFusionError> {
        self.inner.copy(src, dst).await
    }

    async fn remove_dir_all(&self, path: &Path) -> Result<(), DataFusionError> {
        self.inner.remove_dir_all(path).await
    }

    async fn mkdir_all(&self, path: &Path) -> Result<(), DataFusionError> {
        self.inner.mkdir_all(path).await
    }

    fn get_physical_path(&self, path: &Path) -> Result<PathBuf, DataFusionError> {
        self.inner.get_physical_path(path)
    }
}

pub struct RocksDBStateBackend {
    state_id: String,
    root_dir: OsString,
    partitions: PartitionRange,
    remote_file_system: Arc<dyn FileSystemStorage + Send + Sync>,
    local_file_system: Arc<dyn FileSystemStorage + Send + Sync>,
    working_dir: OsString,
    // The DB is almost never None, but we sometimes need to drop it to create a new one
    open_db: rocksdb::DB,
    background_task: JoinHandle<()>,
    background_sync_channel: Sender<OsString>,
}

impl RocksDBStateBackend {
    pub async fn open_new(
        state_id: String,
        partitions: PartitionRange,
        remote_file_system: Arc<dyn FileSystemStorage + Send + Sync>,
        local_file_system: Arc<dyn FileSystemStorage + Send + Sync>,
    ) -> Result<Self, DataFusionError> {
        let root_dir = Self::create_root_directory_path(&state_id, &partitions);

        // Create the initial main directories to prevent directory does not exist errors
        local_file_system.mkdir_all(&Self::base_working_directory_path(&root_dir)).await?;
        local_file_system.mkdir_all(&Self::base_checkpoint_directory_path(&root_dir)).await?;

        // Open the main database
        let working_dir = Self::create_working_directory_path(&root_dir);
        let db = Self::open_rocksdb_database(local_file_system.as_ref(), &working_dir)?;

        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        let background_task = tokio::spawn(RocksDBStateBackend::async_checkpoint_background_task(
            receiver,
            local_file_system.clone(),
            remote_file_system.clone(),
        ));

        Ok(Self {
            state_id,
            root_dir: root_dir.into_os_string(),
            partitions,
            remote_file_system,
            local_file_system,
            working_dir: working_dir.into_os_string(),
            open_db: db,
            background_sync_channel: sender,
            background_task,
        })
    }

    // pub async fn open_from(
    //     state_id: String,
    //     checkpoint: usize,
    //     partitions: PartitionRange,
    //     file_system: Box<dyn FileSystemStorage>,
    // ) -> Self {
    //     // Iterate through each saved db instance, find the ones that match the checkpoint and
    //     // partition range, download the files, and clone them into a new RocksDB instance.
    // }

    pub fn put(&mut self, k: impl AsRef<[u8]>, v: impl AsRef<[u8]>) -> Result<(), DataFusionError> {
        self.open_db.put(k, v)
            .map_err(|e| internal_datafusion_err!("Failed to put key-value pair in RocksDB: {}", e))
    }

    pub fn get(&mut self, k: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>, DataFusionError> {
        self.open_db.get(k)
            .map_err(|e| internal_datafusion_err!("Failed to get key from RocksDB: {}", e))
    }

    pub async fn checkpoint(&mut self, checkpoint: usize) -> Result<(), DataFusionError> {
        // It is necessary to flush all database mem-tables to disk before creating a checkpoint
        self.open_db.flush()
            .map_err(|e| internal_datafusion_err!("Failed to flush RocksDB: {}", e))?;

        // Creates a new directory, and writes the checkpoint to it. Rocksdb throws an error if the
        // directory already exists.
        let checkpoint_dir = Self::create_checkpoint_directory_path(&self.root_dir, checkpoint);
        self.create_rocksdb_checkpoint(&checkpoint_dir)?;

        // Sends the checkpoint to the background task to persist it to the remote file system
        self.background_sync_channel.send(checkpoint_dir.into_os_string()).await
            .map_err(|e| internal_datafusion_err!("Failed to send checkpoint to background task: {}", e))?;

        Ok(())
    }

    pub async fn move_to_checkpoint(&mut self, checkpoint: usize) -> Result<(), DataFusionError> {
        // Find the directory for the checkpoint with a fairly inefficient search
        let (checkpoint_path, is_local) = self.find_checkpoint(checkpoint).await?;

        // Copy the checkpoint into another working directory
        if !is_local {
            // If the checkpoint is remote, we need to download it first. This is also inefficient
            // as we end up downloading the checkpoint before copying it to the working directory.
            Self::copy_dir_between_systems(self.remote_file_system.as_ref(), self.local_file_system.as_ref(), &checkpoint_path).await?;
        }
        // If the checkpoint is local, we can just copy it directly
        let new_working_dir = Self::create_working_directory_path(&self.root_dir);
        self.local_file_system.copy(&checkpoint_path, &new_working_dir).await?;

        let new_db = Self::open_rocksdb_database(self.local_file_system.as_ref(), &new_working_dir)?;
        // Clean up the old working directory
        let old_working_dir = mem::replace(&mut self.working_dir, new_working_dir.into_os_string());
        let old_db = mem::replace(&mut self.open_db, new_db);
        drop(old_db);
        self.destroy_rocksdb(&old_working_dir)?;

        Ok(())
    }

    async fn async_checkpoint_background_task(
        mut checkpoints: tokio::sync::mpsc::Receiver<OsString>,
        local_file_system: impl AsRef<dyn FileSystemStorage + Send + Sync>,
        remote_file_system: impl AsRef<dyn FileSystemStorage + Send + Sync>,
    ) {
        // Uploads checkpoints in the background to the remote file system
        while let Some(next_path) = checkpoints.recv().await {
            Self::copy_dir_between_systems(
                local_file_system.as_ref(),
                remote_file_system.as_ref(),
                &next_path,
            ).await.unwrap_or_else(|e| {
                eprintln!("Failed to upload checkpoint directory {}: {}", next_path.to_string_lossy(), e);
            });
        }
    }

    async fn copy_dir_between_systems(
        source_system: &(dyn FileSystemStorage + Send + Sync),
        destination_system: &(dyn FileSystemStorage + Send + Sync),
        path: impl AsRef<Path>,
    ) -> Result<(), DataFusionError> {
        destination_system.mkdir_all(&path.as_ref()).await?;
        let mut queue = vec![PathBuf::new().join(path.as_ref())];
        while let Some(next) = queue.pop() {
            for entry in source_system.list_files(&next).await? {
                let entry_path = next.join(&entry.name);
                if entry.directory {
                    destination_system.mkdir_all(&entry_path).await?;
                    queue.push(entry_path);
                } else {
                    let contents = source_system.read_file(&entry_path).await?;
                    destination_system.write_file(&entry_path, &contents).await?;
                }
            }
        }

        Ok(())
    }

    async fn find_checkpoint(&self, checkpoint: usize) -> Result<(PathBuf, bool), DataFusionError> {
        let checkpoint_root_dir = Self::base_checkpoint_directory_path(&self.root_dir);

        // Find the checkpoint in the local file system first. This is fairly inefficient
        let checkpoint_files = self.local_file_system.list_files(&checkpoint_root_dir).await?;
        let checkpoint_dir = checkpoint_files
            .iter()
            .find(|entry| entry.name.to_str().map_or(false, |s| s.starts_with(&format!("{}__", checkpoint))));
        if let Some(checkpoint_dir) = checkpoint_dir {
            let checkpoint_path = checkpoint_root_dir.join(&checkpoint_dir.name);
            println!("Moving to checkpoint at {}", self.local_file_system.get_physical_path(&checkpoint_path)?.display());
            return Ok((checkpoint_path, true));
        }

        // If no local checkpoint was found, try to find it in the remote file system
        let checkpoint_files = self.remote_file_system.list_files(&checkpoint_root_dir).await?;
        let checkpoint_dir = checkpoint_files
            .iter()
            .find(|entry| entry.name.to_str().map_or(false, |s| s.starts_with(&format!("{}__", checkpoint))));
        if let Some(checkpoint_dir) = checkpoint_dir {
            let checkpoint_path = checkpoint_root_dir.join(&checkpoint_dir.name);
            println!("Moving to remote checkpoint at {}", self.remote_file_system.get_physical_path(&checkpoint_path)?.display());
            return Ok((checkpoint_path, false));
        }

        Err(internal_datafusion_err!("RocksDB state backend: checkpoint {} not found locally or in remote file system. Available checkpoints: {:?}", checkpoint, checkpoint_files))
    }

    fn create_rocksdb_checkpoint(&self, checkpoint_dir: &PathBuf) -> Result<(), DataFusionError> {
        let absolute_checkpoint_dir = self.local_file_system.get_physical_path(&checkpoint_dir)?;
        Checkpoint::new(&self.open_db)
            .map_err(|e| internal_datafusion_err!("Failed to create checkpoint: {}", e))?
            .create_checkpoint(&absolute_checkpoint_dir)
            .map_err(|e| internal_datafusion_err!("Failed to create checkpoint at {}: {}", absolute_checkpoint_dir.display(), e))?;
        Ok(())
    }

    fn open_rocksdb_database(file_system: &dyn FileSystemStorage, working_dir: impl AsRef<Path>) -> Result<rocksdb::DB, DataFusionError> {
        let absolute_working_dir = file_system.get_physical_path(working_dir.as_ref())?;
        rocksdb::DB::open_default(&absolute_working_dir)
            .map_err(|e| internal_datafusion_err!("Failed to open RocksDB: {}", e))
    }

    fn destroy_rocksdb(&mut self, working_dir: impl AsRef<Path>) -> Result<(), DataFusionError> {
        let options = rocksdb::Options::default();
        let absolute_old_working_dir = self.local_file_system.get_physical_path(working_dir.as_ref())?;
        rocksdb::DB::destroy(&options, &absolute_old_working_dir)
            .map_err(|e| internal_datafusion_err!("Failed to destroy old RocksDB at {}: {}", working_dir.as_ref().to_string_lossy(), e))
    }

    fn create_root_directory_path(state_id: &String, partitions: &PartitionRange) -> PathBuf {
        Path::new(state_id).join(format!(
            "partition-{}-{}-{}",
            partitions.start(),
            partitions.end(),
            partitions.partitions(),
        ))
    }

    fn base_working_directory_path(root_dir: impl AsRef<Path>) -> PathBuf {
        root_dir.as_ref().join("working")
    }

    fn create_working_directory_path(root_dir: impl AsRef<Path>) -> PathBuf {
        Self::base_working_directory_path(&root_dir).join(uuid::Uuid::new_v4().to_string())
    }

    fn base_checkpoint_directory_path(root_dir: impl AsRef<Path>) -> PathBuf {
        root_dir.as_ref().join("checkpoint")
    }

    fn create_checkpoint_directory_path(root_dir: impl AsRef<Path>, checkpoint: usize) -> PathBuf {
        Self::base_checkpoint_directory_path(&root_dir)
            .join(format!("{}__{}", checkpoint, uuid::Uuid::new_v4()))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use rocksdb::{DB, Options, IteratorMode, IngestExternalFileOptions, SstFileWriter};
    use rocksdb::checkpoint::Checkpoint;
    use crate::streaming::partitioning::PartitionRange;
    use crate::streaming::state::state::{PrefixedLocalFileSystemStorage, RocksDBStateBackend};

    #[tokio::test]
    async fn storage_test() {
        let database_dir = make_temp_dir();
        let backup_dir = make_temp_dir();

        let database_file_system = PrefixedLocalFileSystemStorage::new(database_dir.path());
        let backup_file_system = PrefixedLocalFileSystemStorage::new(backup_dir.path());

        let mut backend = RocksDBStateBackend::open_new(
            "test_state".to_string(),
            PartitionRange::new(0, 10, 100),
            Arc::new(database_file_system),
            Arc::new(backup_file_system),
        ).await.unwrap();

        backend.put(b"key1", b"value1").unwrap();
        assert_eq!(backend.get("key1").unwrap(), Some(b"value1".to_vec()));
        assert_eq!(backend.get("key2").unwrap(), None);

        backend.checkpoint(1).await.unwrap();
        backend.put(b"key2", b"value2").unwrap();
        assert_eq!(backend.get("key2").unwrap(), Some(b"value2".to_vec()));

        backend.move_to_checkpoint(1).await.unwrap();
        assert_eq!(backend.get("key1").unwrap(), Some(b"value1".to_vec()));
        assert_eq!(backend.get("key2").unwrap(), None);
    }

    fn make_temp_dir() -> tempfile::TempDir {
        tempfile::Builder::new()
            .prefix("_path_for_rocksdb_storage")
            .tempdir()
            .expect("Failed to create temporary path for the _path_for_rocksdb_storage")
    }

    fn test() {
        let tempdir = tempfile::Builder::new()
            .prefix("_path_for_rocksdb_storage")
            .tempdir()
            .expect("Failed to create temporary path for the _path_for_rocksdb_storage");
        let path = tempdir.path();
        println!("Temp path for RocksDB storage: {}", path.display());

        // Create the first database
        {
            let db = DB::open_default(path).unwrap();
            db.put(b"my key", b"my value").unwrap();
            for i in 0..1000 {
                db.put(format!("key {}", i).as_bytes(), format!("value {}", i).as_bytes()).unwrap();
            }

            println!("Wrote all keys");
            println!("Live files: {:?}", db.live_files().unwrap());
            println!("Flushing");
            db.flush().unwrap();
            println!("Live files: {:?}", db.live_files().unwrap());

            match db.get(b"my key") {
                Ok(Some(value)) => println!("retrieved value {}", String::from_utf8(value).unwrap()),
                Ok(None) => println!("value not found"),
                Err(e) => println!("operational problem encountered: {}", e),
            }
        }

        let checkpoint_tempdir = tempfile::Builder::new()
            .prefix("_path_for_rocksdb_storage")
            .tempdir()
            .expect("Failed to create temporary path for the _path_for_rocksdb_storage");
        let checkpoint_path = checkpoint_tempdir.path().join("checkpoint");

        // Try to reopen the database at the same path
        {
            println!("Reopening the database at the same path");
            let db = DB::open_default(path).unwrap();
            println!("Live files: {:?}", db.live_files().unwrap());

            match db.get(b"my key") {
                Ok(Some(value)) => println!("retrieved value {}", String::from_utf8(value).unwrap()),
                Ok(None) => println!("value not found"),
                Err(e) => println!("operational problem encountered: {}", e),
            }
            db.delete(b"my key").unwrap();
            db.flush().unwrap();
            println!("Live files: {:?}", db.live_files().unwrap());
            // db.live_files().unwrap()

            // Create a checkpoint
            Checkpoint::new(&db).unwrap()
                .create_checkpoint(&checkpoint_path)
                .unwrap();
        };

        // List all the files in the directory
        println!("Listing all files in the db directory:");
        for entry in std::fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            println!("File: {}", entry.path().display());
        }

        // List all the files in the checkpoint directory
        println!("Listing all files in the checkpoint directory:");
        for entry in std::fs::read_dir(&checkpoint_path).unwrap() {
            let entry = entry.unwrap();
            println!("File: {}", entry.path().display());
        }

        // Reopen the checkpoint as a database
        {
            println!("Reopening the checkpoint as a database");
            let db = DB::open_default(&checkpoint_path).unwrap();
            println!("Live files: {:?}", db.live_files().unwrap());
            println!("Current entries in second db {:?}", db.iterator(IteratorMode::Start).count());

            db.delete(b"my key").unwrap();
            db.put(b"new key", b"new value").unwrap();
            db.flush().unwrap();
        }

        // List all the files in the checkpoint directory
        println!("Listing all files in the checkpoint directory:");
        for entry in std::fs::read_dir(&checkpoint_path).unwrap() {
            let entry = entry.unwrap();
            println!("File: {}", entry.path().display());
        }

        // Creating a new db
        let second_tempdir = tempfile::Builder::new()
            .prefix("_path_for_rocksdb_storage")
            .tempdir()
            .expect("Failed to create temporary path for the _path_for_rocksdb_storage");
        let second_path = second_tempdir.path();
        println!("Second temp path for RocksDB storage: {}", second_path.display());

        let sst_tempdir = tempfile::Builder::new()
            .prefix("_path_for_rocksdb_storage")
            .tempdir()
            .expect("Failed to create temporary path for the _path_for_rocksdb_storage");
        let sst_path = sst_tempdir.path().join("exported.sst");

        {
            let old_db = DB::open_default(path).unwrap();


            // Export SSTs from the old database to fresh files
            let sst_writer_options = Options::default();
            let mut sst_writer = SstFileWriter::create(&sst_writer_options);
            sst_writer.open(&sst_path).unwrap();
            for entry in old_db.iterator(IteratorMode::Start) {
                let (key, value) = entry.unwrap();
                sst_writer.put(&key, &value).unwrap();
            }
            sst_writer.finish().unwrap();
            println!("Exported SST file");


            let mut open_options = Options::default();
            open_options.prepare_for_bulk_load();
            open_options.create_if_missing(true);
            let db = DB::open(&open_options, second_path).unwrap();

            // Now ingest the newly created SST file
            let mut ingest_opts = IngestExternalFileOptions::default();
            ingest_opts.set_move_files(true);
            db.ingest_external_file_opts(&ingest_opts, vec![&sst_path]).unwrap();
            // Explicitly compact database as recommended by prepare_for_bulk_load
            db.compact_range::<Vec<u8>, Vec<u8>>(None, None);
            drop(db);

            // Reopen the database with default options (I have no idea if this is necessary)
            let db = DB::open_default(second_path).unwrap();
            println!("Live files: {:?}", db.live_files().unwrap());
            println!("Current entries in second db {:?}", db.iterator(IteratorMode::Start).count());
        }

        let _ = DB::destroy(&Options::default(), second_path);
        let _ = DB::destroy(&Options::default(), path);
    }
}
