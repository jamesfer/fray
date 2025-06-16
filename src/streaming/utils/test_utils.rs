use std::ffi::OsStr;
use std::sync::Arc;
use local_ip_address::local_ip;
use datafusion::common::internal_datafusion_err;
use datafusion::error::DataFusionError;
use crate::streaming::runtime::Runtime;
use crate::streaming::state::state::TempdirFileSystemStorage;

pub fn make_temp_dir(prefix: impl AsRef<OsStr>) -> Result<tempfile::TempDir, DataFusionError> {
    tempfile::Builder::new()
        .prefix(&prefix)
        .tempdir()
        .map_err(|e| internal_datafusion_err!("Failed to create temporary directory: {}", e))
}

pub async fn make_test_runtime() -> Result<Arc<Runtime>, DataFusionError> {
    let local_ip_address = local_ip()
        .map_err(|e| internal_datafusion_err!("Failed to get local IP address: {}", e))?;
    let local_file_system = TempdirFileSystemStorage::from_tempdir(
        make_temp_dir("test_runtime_local_fs")?
    );
    let remote_file_system = TempdirFileSystemStorage::from_tempdir(
        make_temp_dir("test_runtime_remote_fs")?
    );
    let runtime = Runtime::start(
        local_ip_address,
        Arc::new(local_file_system),
        Arc::new(remote_file_system),
    ).await?;
    Ok(Arc::new(runtime))
}
