//! Error types for snapshot creation.

use std::io;

/// Result type for snapshot operations.
pub type SnapshotResult<T> = Result<T, SnapshotError>;

/// Errors that can occur during snapshot creation.
#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    /// I/O error during file operations.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// Database file not found.
    #[error("Database file not found")]
    DatabaseNotFound,

    /// Archive creation failed.
    #[error("Archive creation error: {0}")]
    ArchiveCreation(String),
}
