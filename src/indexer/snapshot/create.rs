//! Snapshot creation for exporting database to tar.xz archives.

use std::{
    fs::{self, File},
    path::Path,
};

use tracing::{debug, info};
use xz2::write::XzEncoder;

use super::error::{SnapshotError, SnapshotResult};

/// Creates tar.xz snapshot archives from database files.
pub struct SnapshotCreator;

impl SnapshotCreator {
    /// Creates a new snapshot creator.
    pub fn new() -> Self {
        Self
    }

    /// Creates a tar.xz snapshot archive from a database file.
    ///
    /// # Arguments
    ///
    /// * `db_path` - Path to the SQLite database file to archive
    /// * `output_path` - Destination path for the tar.xz archive
    ///
    /// # Returns
    ///
    /// Size of the created archive in bytes
    pub fn create_snapshot(&self, db_path: &Path, output_path: &Path) -> SnapshotResult<u64> {
        info!(
            "Creating snapshot: {} -> {}",
            db_path.display(),
            output_path.display()
        );

        if !db_path.exists() {
            return Err(SnapshotError::DatabaseNotFound);
        }

        // Ensure output directory exists
        if let Some(parent) = output_path.parent() {
            fs::create_dir_all(parent)?;
        }

        // Create tar.xz archive
        let file = File::create(output_path)?;
        let encoder = XzEncoder::new(file, 6);
        let mut tar = tar::Builder::new(encoder);

        // Add main database file as hopr_logs.db
        debug!("Adding database file as hopr_logs.db");
        tar.append_path_with_name(db_path, "hopr_logs.db")?;

        // Add WAL file if it exists (check both .db-wal and .sqlite3-wal)
        let wal_path = db_path.parent().unwrap().join(
            format!("{}-wal", db_path.file_name().unwrap().to_string_lossy())
        );
        if wal_path.exists() {
            debug!("Adding WAL file as hopr_logs.db-wal");
            tar.append_path_with_name(&wal_path, "hopr_logs.db-wal")?;
        }

        // Add SHM file if it exists (check both .db-shm and .sqlite3-shm)
        let shm_path = db_path.parent().unwrap().join(
            format!("{}-shm", db_path.file_name().unwrap().to_string_lossy())
        );
        if shm_path.exists() {
            debug!("Adding SHM file as hopr_logs.db-shm");
            tar.append_path_with_name(&shm_path, "hopr_logs.db-shm")?;
        }

        tar.finish()?;

        let size = fs::metadata(output_path)?.len();
        info!("Snapshot created: {} bytes", size);

        Ok(size)
    }
}
