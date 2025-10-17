//! SQLite helper for persisting decoded HOPR activity.

use alloy_primitives::{Address, B256};
use eyre::WrapErr;
use rusqlite::{params, Connection, OpenFlags};
use std::path::Path;

/// Schema definition used by the HOPR indexer (from migrations).
pub const HOPR_DB_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS log_status (
    transaction_index BLOB(8) NOT NULL,
    log_index BLOB(8) NOT NULL,
    block_number BLOB(8) NOT NULL,
    processed BOOLEAN NOT NULL DEFAULT FALSE,
    processed_at DATETIME,
    checksum BLOB(32),
    PRIMARY KEY (block_number, transaction_index, log_index)
);

CREATE TABLE IF NOT EXISTS log (
    transaction_index BLOB(8) NOT NULL,
    log_index BLOB(8) NOT NULL,
    block_number BLOB(8) NOT NULL,
    block_hash BLOB(32) NOT NULL,
    transaction_hash BLOB(32) NOT NULL,
    address BLOB(20) NOT NULL,
    topics BLOB NOT NULL,
    data BLOB NOT NULL,
    removed BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (block_number, transaction_index, log_index),
    FOREIGN KEY (block_number, transaction_index, log_index)
        REFERENCES log_status (block_number, transaction_index, log_index)
        ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE IF NOT EXISTS log_topic_info (
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    address VARCHAR(40) NOT NULL,
    topic VARCHAR(64) NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_contract_log_topic ON log_topic_info (address, topic);

CREATE TABLE IF NOT EXISTS seaql_migrations (
    version VARCHAR NOT NULL PRIMARY KEY,
    applied_at BIGINT NOT NULL
);
"#;

/// Thin wrapper around a rusqlite [`Connection`] with helper routines tailored for the
/// HOPR indexer tables.
#[derive(Debug)]
pub struct HoprEventsDb {
    conn: Connection,
}

impl HoprEventsDb {
    /// Opens (or creates) a SQLite database at the provided path and ensures the HOPR schema exists.
    pub fn open(path: impl AsRef<Path>) -> eyre::Result<Self> {
        let flags = OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE;
        let conn = Connection::open_with_flags(path.as_ref(), flags)
            .wrap_err("failed to open hopr events database")?;
        Self::configure(&conn, true)?;
        let db = Self { conn };
        db.ensure_chain_info_row()?;
        Ok(db)
    }

    /// Creates an in-memory database; primarily useful for tests.
    pub fn open_in_memory() -> eyre::Result<Self> {
        let conn = Connection::open_in_memory().wrap_err("failed to open in-memory database")?;
        Self::configure(&conn, false)?;
        let db = Self { conn };
        db.ensure_chain_info_row()?;
        Ok(db)
    }

    /// Returns an immutable reference to the underlying connection.
    pub fn connection(&self) -> &Connection {
        &self.conn
    }

    /// Returns a mutable reference to the underlying connection.
    pub fn connection_mut(&mut self) -> &mut Connection {
        &mut self.conn
    }

    fn configure(conn: &Connection, persistent: bool) -> eyre::Result<()> {
        conn.pragma_update(None, "foreign_keys", &1)
            .wrap_err("failed to enable foreign_keys pragma")?;
        if persistent {
            conn.pragma_update(None, "journal_mode", &"WAL")
                .wrap_err("failed to set journal_mode to WAL")?;
        }
        conn.pragma_update(None, "synchronous", &"NORMAL")
            .wrap_err("failed to set synchronous pragma")?;
        conn.execute_batch(HOPR_DB_SCHEMA)
            .wrap_err("failed to initialize hopr schema")?;
        Ok(())
    }

    /// No-op for compatibility (log_status is populated per log entry now).
    pub fn ensure_chain_info_row(&self) -> eyre::Result<()> {
        // Log status entries are created per log, no global initialization needed
        Ok(())
    }

    /// No-op for compatibility (log_status is updated per log entry now).
    pub fn update_last_indexed_block(&self, _block_number: u64) -> eyre::Result<()> {
        // Status is tracked per log entry, not globally
        Ok(())
    }

    /// Persists a raw log entry emitted by the HOPR contracts.
    pub fn record_raw_log(
        &self,
        block_number: u64,
        tx_index: usize,
        log_index: usize,
        address: Address,
        topics: &[B256],
        data: &[u8],
        _event_name: &str,
    ) -> eyre::Result<()> {
        // Encode topics as concatenated blob
        let topics_blob: Vec<u8> = topics.iter().flat_map(|t| t.as_slice()).copied().collect();

        // Encode indices as 8-byte big-endian blobs
        let tx_index_bytes = (tx_index as u64).to_be_bytes();
        let log_index_bytes = (log_index as u64).to_be_bytes();
        let block_number_bytes = block_number.to_be_bytes();

        // For now, use zero hashes for block_hash and transaction_hash
        let block_hash = vec![0u8; 32];
        let transaction_hash = vec![0u8; 32];

        // First insert log_status (required by foreign key constraint)
        self.conn
            .execute(
                "INSERT OR REPLACE INTO log_status \
                (transaction_index, log_index, block_number, processed, processed_at, checksum) \
                VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    &tx_index_bytes[..],
                    &log_index_bytes[..],
                    &block_number_bytes[..],
                    false,
                    None::<String>,
                    None::<Vec<u8>>
                ],
            )
            .wrap_err("failed to persist log_status")?;

        // Then insert log entry
        self.conn
            .execute(
                "INSERT OR REPLACE INTO log \
                (transaction_index, log_index, block_number, block_hash, transaction_hash, address, topics, data, removed) \
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    &tx_index_bytes[..],
                    &log_index_bytes[..],
                    &block_number_bytes[..],
                    &block_hash[..],
                    &transaction_hash[..],
                    address.as_slice(),
                    &topics_blob[..],
                    data,
                    false
                ],
            )
            .wrap_err("failed to persist log")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::OptionalExtension;

    #[test]
    fn schema_initializes() {
        let db = HoprEventsDb::open_in_memory().expect("in-memory db");
        let mut stmt = db
            .connection()
            .prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'log'")
            .expect("prepare statement");
        let table_exists: Option<String> = stmt
            .query_row([], |row| row.get(0))
            .optional()
            .expect("query sqlite_master");
        assert_eq!(table_exists.as_deref(), Some("log"));
    }

    #[test]
    fn raw_log_persists() {
        let db = HoprEventsDb::open_in_memory().expect("in-memory db");
        db.record_raw_log(1, 0, 0, Address::ZERO, &[], &[], "TestEvent")
            .expect("record raw log");
        let count: i64 = db
            .connection()
            .query_row("SELECT COUNT(*) FROM log", [], |row| row.get(0))
            .expect("row count");
        assert_eq!(count, 1);
    }

    #[test]
    fn log_status_created_with_log() {
        let db = HoprEventsDb::open_in_memory().expect("in-memory db");

        // Record a log
        db.record_raw_log(1, 0, 0, Address::ZERO, &[], &[], "TestEvent")
            .expect("record raw log");

        // Verify log_status entry was created
        let (processed, checksum_null): (bool, bool) = db
            .connection()
            .query_row(
                "SELECT processed, checksum IS NULL FROM log_status WHERE block_number = x'0000000000000001'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("query log_status");
        assert_eq!(processed, false);
        assert_eq!(checksum_null, true);
    }
}
