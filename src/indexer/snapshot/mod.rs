//! Database snapshot creation.
//!
//! This module provides functionality to export SQLite database files
//! to compressed tar.xz archives for backup and distribution.
//!
//! # Example
//!
//! ```no_run
//! use std::path::Path;
//! use reth_gnosis::indexer::snapshot::SnapshotCreator;
//!
//! # fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let creator = SnapshotCreator::new();
//! let size = creator.create_snapshot(
//!     Path::new("/data/hopr_logs.db"),
//!     Path::new("/backups/snapshot.tar.xz")
//! )?;
//! println!("Created snapshot: {} bytes", size);
//! # Ok(())
//! # }
//! ```

pub mod create;
pub mod error;

pub use create::SnapshotCreator;
pub use error::{SnapshotError, SnapshotResult};
