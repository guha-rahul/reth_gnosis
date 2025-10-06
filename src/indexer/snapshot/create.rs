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

        // Get database filename for archive
        let db_filename = db_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("hopr_logs.db");

        // Create tar.xz archive
        let file = File::create(output_path)?;
        let encoder = XzEncoder::new(file, 6);
        let mut tar = tar::Builder::new(encoder);

        debug!("Adding {} to archive", db_filename);
        tar.append_path_with_name(db_path, db_filename)?;
        tar.finish()?;

        let size = fs::metadata(output_path)?.len();
        info!("Snapshot created: {} bytes", size);

        Ok(size)
    }
}
