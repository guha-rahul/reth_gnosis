//! SQLite helper for persisting decoded HOPR activity.

use alloy_primitives::{Address, B256};
use eyre::WrapErr;
use rusqlite::{params, Connection, OpenFlags};
use std::path::Path;

/// Schema definition used by the HOPR indexer.
pub const HOPR_DB_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS "seaql_migrations" ( "version" varchar NOT NULL PRIMARY KEY, "applied_at" bigint NOT NULL );
CREATE TABLE IF NOT EXISTS "channel" ( "id" integer NOT NULL PRIMARY KEY AUTOINCREMENT, "channel_id" varchar(64) NOT NULL UNIQUE, "source" varchar(40) NOT NULL, "destination" varchar(40) NOT NULL, "balance" blob(12) NOT NULL, "status" tinyint NOT NULL, "epoch" blob(8) NOT NULL DEFAULT x'0000000000000000000000000000000000000000000000000000000000000001', "ticket_index" blob(8) NOT NULL DEFAULT x'0000000000000000000000000000000000000000000000000000000000000000', "closure_time" timestamp_text NULL , "corrupted" boolean NOT NULL DEFAULT FALSE);
CREATE UNIQUE INDEX "idx_channel_id_channel_epoch" ON "channel" ("channel_id", "epoch");
CREATE UNIQUE INDEX "idx_channel_source_destination" ON "channel" ("source", "destination");
CREATE INDEX "idx_channel_source" ON "channel" ("source");
CREATE INDEX "idx_channel_destination" ON "channel" ("destination");
CREATE TABLE IF NOT EXISTS "account" ( "id" integer NOT NULL PRIMARY KEY AUTOINCREMENT, "chain_key" varchar(40) NOT NULL, "packet_key" varchar(64) NOT NULL , "published_at" integer NOT NULL DEFAULT 0);
CREATE INDEX "idx_account_chain_key" ON "account" ("chain_key");
CREATE INDEX "idx_account_packet_key" ON "account" ("packet_key");
CREATE UNIQUE INDEX "idx_account_chain_packet_key" ON "account" ("chain_key", "packet_key");
CREATE TABLE IF NOT EXISTS "announcement" ( "id" integer NOT NULL PRIMARY KEY AUTOINCREMENT, "account_id" integer NOT NULL, "multiaddress" varchar NOT NULL, "at_block" integer NOT NULL, FOREIGN KEY ("account_id") REFERENCES "account" ("id") ON DELETE CASCADE ON UPDATE RESTRICT );
CREATE INDEX "idx_announcement_account_id" ON "announcement" ("account_id");
CREATE UNIQUE INDEX "idx_announcement_account_id_multiaddress" ON "announcement" ("account_id", "multiaddress");
CREATE TABLE IF NOT EXISTS "network_registry" ( "id" integer NOT NULL PRIMARY KEY AUTOINCREMENT, "chain_address" varchar(40) UNIQUE NOT NULL );
CREATE TABLE IF NOT EXISTS "node_info" ( "id" integer NOT NULL PRIMARY KEY AUTOINCREMENT, "safe_balance" blob(12) NOT NULL DEFAULT x'000000000000000000000000', "safe_allowance" blob(12) NOT NULL DEFAULT x'000000000000000000000000', "safe_address" varchar(40) NULL, "module_address" varchar(40) NULL );
CREATE TABLE IF NOT EXISTS "chain_info" ( "id" integer NOT NULL PRIMARY KEY AUTOINCREMENT, "last_indexed_block" integer NOT NULL DEFAULT 0, "ticket_price" blob(12) NULL, "channels_dst" blob(32) NULL, "ledger_dst" blob(32) NULL, "safe_registry_dst" blob(32) NULL, "network_registry_enabled" boolean NOT NULL DEFAULT FALSE, "chain_checksum" blob(32) DEFAULT x'0000000000000000000000000000000000000000000000000000000000000000' , "previous_indexed_block_prio_to_checksum_update" integer NOT NULL DEFAULT 0, "min_incoming_ticket_win_prob" float NOT NULL DEFAULT 1);
CREATE TABLE IF NOT EXISTS "network_eligibility" ( "id" integer NOT NULL PRIMARY KEY AUTOINCREMENT, "safe_address" varchar NOT NULL UNIQUE );
CREATE TABLE IF NOT EXISTS "global_settings" ( "id" integer NOT NULL PRIMARY KEY AUTOINCREMENT, "key" varchar(80) UNIQUE NOT NULL, "value" blob(1) NOT NULL );
CREATE INDEX "idx_channel_closure_time" ON "channel" ("closure_time" ASC);
CREATE INDEX "idx_channel_status" ON "channel" ("status" ASC);
CREATE TABLE IF NOT EXISTS "hopr_raw_logs" (
    "block_number" integer NOT NULL,
    "tx_index" integer NOT NULL,
    "log_index" integer NOT NULL,
    "address" blob NOT NULL,
    "topic0" blob NULL,
    "topic1" blob NULL,
    "topic2" blob NULL,
    "topic3" blob NULL,
    "data" blob NOT NULL,
    "event_name" text NOT NULL,
    PRIMARY KEY ("block_number", "tx_index", "log_index")
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

    /// Ensures that a default row for `chain_info` exists so metadata updates can be applied via
    /// simple `UPDATE` statements.
    pub fn ensure_chain_info_row(&self) -> eyre::Result<()> {
        self.conn
            .execute(
                "INSERT OR IGNORE INTO chain_info (id, last_indexed_block) VALUES (1, 0)",
                [],
            )
            .wrap_err("failed to seed chain_info row")?;
        Ok(())
    }

    /// Updates `chain_info.last_indexed_block` to the provided block number.
    pub fn update_last_indexed_block(&self, block_number: u64) -> eyre::Result<()> {
        self.conn
            .execute(
                "UPDATE chain_info SET last_indexed_block = ?1 WHERE id = 1",
                params![block_number as i64],
            )
            .wrap_err("failed to update last indexed block")?;
        Ok(())
    }

    /// Persists a raw log entry emitted by the HOPR contracts for future decoding/indexing.
    pub fn record_raw_log(
        &self,
        block_number: u64,
        tx_index: usize,
        log_index: usize,
        address: Address,
        topics: &[B256],
        data: &[u8],
        event_name: &str,
    ) -> eyre::Result<()> {
        let topic_bytes = |idx: usize| -> Option<Vec<u8>> {
            topics.get(idx).map(|topic| topic.as_slice().to_vec())
        };

        let topic0 = topic_bytes(0);
        let topic1 = topic_bytes(1);
        let topic2 = topic_bytes(2);
        let topic3 = topic_bytes(3);

        self.conn
            .execute(
                "INSERT OR REPLACE INTO hopr_raw_logs \
                (block_number, tx_index, log_index, address, topic0, topic1, topic2, topic3, data, event_name) \
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                params![
                    block_number as i64,
                    tx_index as i64,
                    log_index as i64,
                    address.as_slice(),
                    topic0.as_deref(),
                    topic1.as_deref(),
                    topic2.as_deref(),
                    topic3.as_deref(),
                    data,
                    event_name
                ],
            )
            .wrap_err("failed to persist raw hopr log")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::{params, OptionalExtension};

    #[test]
    fn schema_initializes() {
        let db = HoprEventsDb::open_in_memory().expect("in-memory db");
        let mut stmt = db
            .connection()
            .prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'channel'")
            .expect("prepare statement");
        let table_exists: Option<String> = stmt
            .query_row([], |row| row.get(0))
            .optional()
            .expect("query sqlite_master");
        assert_eq!(table_exists.as_deref(), Some("channel"));
    }

    #[test]
    fn raw_log_persists() {
        let db = HoprEventsDb::open_in_memory().expect("in-memory db");
        db.record_raw_log(1, 0, 0, Address::ZERO, &[], &[], "TestEvent")
            .expect("record raw log");
        let count: i64 = db
            .connection()
            .query_row("SELECT COUNT(*) FROM hopr_raw_logs", [], |row| row.get(0))
            .expect("row count");
        assert_eq!(count, 1);
    }

    #[test]
    fn chain_info_seed_and_update() {
        let db = HoprEventsDb::open_in_memory().expect("in-memory db");

        // Seeded row exists with id = 1 and last_indexed_block = 0
        let (id, last): (i64, i64) = db
            .connection()
            .query_row(
                "SELECT id, last_indexed_block FROM chain_info WHERE id = 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("seeded chain_info row");
        assert_eq!(id, 1);
        assert_eq!(last, 0);

        // Update and verify
        db.update_last_indexed_block(12345)
            .expect("update last indexed block");
        let last_updated: i64 = db
            .connection()
            .query_row(
                "SELECT last_indexed_block FROM chain_info WHERE id = 1",
                [],
                |row| row.get(0),
            )
            .expect("read updated last_indexed_block");
        assert_eq!(last_updated, 12345);
    }

    #[test]
    fn foreign_key_cascade_on_announcement() {
        let db = HoprEventsDb::open_in_memory().expect("in-memory db");

        // Insert an account
        db.connection()
            .execute(
                "INSERT INTO account (chain_key, packet_key, published_at) VALUES (?1, ?2, 0)",
                params!["0xabcde0000000000000000000000000000000000", "0xdeadbeef"],
            )
            .expect("insert account");

        // Resolve the inserted account id
        let account_id: i64 = db
            .connection()
            .query_row(
                "SELECT id FROM account WHERE chain_key = ?1 AND packet_key = ?2",
                params!["0xabcde0000000000000000000000000000000000", "0xdeadbeef"],
                |row| row.get(0),
            )
            .expect("select account id");

        // Insert a dependent announcement row
        db.connection()
            .execute(
                "INSERT INTO announcement (account_id, multiaddress, at_block) VALUES (?1, ?2, 1)",
                params![account_id, "/ip4/127.0.0.1/tcp/1234"],
            )
            .expect("insert announcement");

        // Delete the account and ensure cascade removes the announcement
        db.connection()
            .execute("DELETE FROM account WHERE id = ?1", params![account_id])
            .expect("delete account");

        let remaining: i64 = db
            .connection()
            .query_row("SELECT COUNT(*) FROM announcement", [], |row| row.get(0))
            .expect("count announcements");
        assert_eq!(remaining, 0);
    }

    #[test]
    fn uniqueness_constraints_account_and_channel() {
        let db = HoprEventsDb::open_in_memory().expect("in-memory db");

        // account unique (chain_key, packet_key)
        db.connection()
            .execute(
                "INSERT INTO account (chain_key, packet_key, published_at) VALUES (?1, ?2, 0)",
                params!["0x1111111111111111111111111111111111111111", "0xpk1"],
            )
            .expect("insert first account");
        let dup = db.connection().execute(
            "INSERT INTO account (chain_key, packet_key, published_at) VALUES (?1, ?2, 0)",
            params!["0x1111111111111111111111111111111111111111", "0xpk1"],
        );
        assert!(
            dup.is_err(),
            "duplicate (chain_key, packet_key) should fail"
        );

        // channel unique channel_id and also (channel_id, epoch)
        db.connection()
            .execute(
                "INSERT INTO channel (channel_id, source, destination, balance, status, epoch, ticket_index) VALUES (?1, ?2, ?3, x'000000000000000000000000', 0, x'0000000000000001', x'0000000000000000')",
                params![
                    "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "0x2222222222222222222222222222222222222222",
                    "0x3333333333333333333333333333333333333333"
                ],
            )
            .expect("insert first channel");
        let dup_channel = db.connection().execute(
            "INSERT INTO channel (channel_id, source, destination, balance, status, epoch, ticket_index) VALUES (?1, ?2, ?3, x'000000000000000000000000', 0, x'0000000000000001', x'0000000000000000')",
            params![
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "0x2222222222222222222222222222222222222222",
                "0x3333333333333333333333333333333333333333"
            ],
        );
        assert!(dup_channel.is_err(), "duplicate channel_id should fail");
    }

    #[test]
    fn hopr_raw_logs_insert_or_replace_on_conflict() {
        let db = HoprEventsDb::open_in_memory().expect("in-memory db");

        db.record_raw_log(10, 1, 2, Address::ZERO, &[], b"abc", "A")
            .expect("insert raw log A");

        // Re-insert same PK with new data and name; count stays 1 and row reflects new values
        db.record_raw_log(10, 1, 2, Address::ZERO, &[], b"xyz", "B")
            .expect("replace raw log with B");

        let (count, event_name, data_hex): (i64, String, String) = db
            .connection()
            .query_row(
                "SELECT COUNT(*), event_name, hex(data) FROM hopr_raw_logs WHERE block_number = 10 AND tx_index = 1 AND log_index = 2",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .expect("select replaced row");
        assert_eq!(count, 1);
        assert_eq!(event_name, "B");
        assert_eq!(data_hex, "78797A"); // hex("xyz")
    }

    #[test]
    fn critical_indexes_exist() {
        let db = HoprEventsDb::open_in_memory().expect("in-memory db");
        let mut stmt = db
            .connection()
            .prepare(
                "SELECT name FROM sqlite_master WHERE type = 'index' AND name IN (
                    'idx_channel_id_channel_epoch',
                    'idx_channel_status',
                    'idx_account_chain_packet_key',
                    'idx_announcement_account_id_multiaddress'
                ) ORDER BY name",
            )
            .expect("prepare index query");
        let mut rows = stmt.query([]).expect("query indexes");
        let mut found: Vec<String> = Vec::new();
        while let Some(row) = rows.next().expect("iterate rows") {
            found.push(row.get::<_, String>(0).expect("index name"));
        }
        // Ensure all listed indexes were created
        assert_eq!(found.len(), 4, "expected 4 critical indexes to exist");
    }

    #[test]
    fn channel_insert_and_select() {
        let db = HoprEventsDb::open_in_memory().expect("in-memory db");
        let channel_id = "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
        db.connection()
            .execute(
                "INSERT INTO channel (channel_id, source, destination, balance, status, epoch, ticket_index) VALUES (?1, ?2, ?3, x'000000000000000000000123', 1, x'0000000000000002', x'0000000000000003')",
                params![
                    channel_id,
                    "0x4444444444444444444444444444444444444444",
                    "0x5555555555555555555555555555555555555555"
                ],
            )
            .expect("insert channel");

        let (count, status): (i64, i64) = db
            .connection()
            .query_row(
                "SELECT COUNT(*), status FROM channel WHERE channel_id = ?1",
                params![channel_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("select channel");
        assert_eq!(count, 1);
        assert_eq!(status, 1);
    }

    #[test]
    fn account_insert_and_select() {
        let db = HoprEventsDb::open_in_memory().expect("in-memory db");
        let chain_key = "0xabc0000000000000000000000000000000000000";
        let packet_key = "0xpk-001";
        db.connection()
            .execute(
                "INSERT INTO account (chain_key, packet_key, published_at) VALUES (?1, ?2, 42)",
                params![chain_key, packet_key],
            )
            .expect("insert account");
        let (id, pub_at): (i64, i64) = db
            .connection()
            .query_row(
                "SELECT id, published_at FROM account WHERE chain_key = ?1 AND packet_key = ?2",
                params![chain_key, packet_key],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("select account");
        assert!(id > 0);
        assert_eq!(pub_at, 42);
    }

    #[test]
    fn announcement_insert_and_select() {
        let db = HoprEventsDb::open_in_memory().expect("in-memory db");
        db.connection()
            .execute(
                "INSERT INTO account (chain_key, packet_key, published_at) VALUES ('0xacc', '0xpk', 0)",
                [],
            )
            .expect("insert account for announcement");
        let account_id: i64 = db
            .connection()
            .query_row(
                "SELECT id FROM account WHERE chain_key = '0xacc'",
                [],
                |row| row.get(0),
            )
            .expect("get account id");
        db.connection()
            .execute(
                "INSERT INTO announcement (account_id, multiaddress, at_block) VALUES (?1, ?2, 99)",
                params![account_id, "/dns4/node/tcp/1111"],
            )
            .expect("insert announcement");
        let (cnt, at_block): (i64, i64) = db
            .connection()
            .query_row(
                "SELECT COUNT(*), at_block FROM announcement WHERE account_id = ?1",
                params![account_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("select announcement");
        assert_eq!(cnt, 1);
        assert_eq!(at_block, 99);
    }

    #[test]
    fn network_registry_insert_and_select() {
        let db = HoprEventsDb::open_in_memory().expect("in-memory db");
        let addr = "0x7777777777777777777777777777777777777777";
        db.connection()
            .execute(
                "INSERT INTO network_registry (chain_address) VALUES (?1)",
                params![addr],
            )
            .expect("insert network_registry");
        let count: i64 = db
            .connection()
            .query_row(
                "SELECT COUNT(*) FROM network_registry WHERE chain_address = ?1",
                params![addr],
                |row| row.get(0),
            )
            .expect("select network_registry");
        assert_eq!(count, 1);
    }

    #[test]
    fn node_info_insert_and_select() {
        let db = HoprEventsDb::open_in_memory().expect("in-memory db");
        db.connection()
            .execute(
                "INSERT INTO node_info (safe_balance, safe_allowance, safe_address, module_address) VALUES (x'00000000000000000000000A', x'00000000000000000000000B', ?1, ?2)",
                params![
                    "0x8888888888888888888888888888888888888888",
                    "0x9999999999999999999999999999999999999999"
                ],
            )
            .expect("insert node_info");
        let (safe_addr, module_addr): (String, String) = db
            .connection()
            .query_row(
                "SELECT safe_address, module_address FROM node_info",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("select node_info");
        assert_eq!(safe_addr, "0x8888888888888888888888888888888888888888");
        assert_eq!(module_addr, "0x9999999999999999999999999999999999999999");
    }

    #[test]
    fn network_eligibility_insert_and_select() {
        let db = HoprEventsDb::open_in_memory().expect("in-memory db");
        let safe = "0xSAFE";
        db.connection()
            .execute(
                "INSERT INTO network_eligibility (safe_address) VALUES (?1)",
                params![safe],
            )
            .expect("insert eligibility");
        let count: i64 = db
            .connection()
            .query_row(
                "SELECT COUNT(*) FROM network_eligibility WHERE safe_address = ?1",
                params![safe],
                |row| row.get(0),
            )
            .expect("select eligibility");
        assert_eq!(count, 1);
    }

    #[test]
    fn global_settings_insert_and_select() {
        let db = HoprEventsDb::open_in_memory().expect("in-memory db");
        db.connection()
            .execute(
                "INSERT INTO global_settings (key, value) VALUES (?1, ?2)",
                params!["feature_x", &vec![1u8][..]],
            )
            .expect("insert settings");
        let (key, val_hex): (String, String) = db
            .connection()
            .query_row(
                "SELECT key, hex(value) FROM global_settings WHERE key = 'feature_x'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("select settings");
        assert_eq!(key, "feature_x");
        assert_eq!(val_hex, "01");
    }

    #[test]
    fn chain_info_update_ticket_price_and_win_prob() {
        let db = HoprEventsDb::open_in_memory().expect("in-memory db");
        db.connection()
            .execute(
                "UPDATE chain_info SET ticket_price = x'0000000000000000000000FF', min_incoming_ticket_win_prob = ?1 WHERE id = 1",
                params![0.25f64],
            )
            .expect("update chain_info extras");
        let (price_hex, win_prob): (String, f64) = db
            .connection()
            .query_row(
                "SELECT hex(ticket_price), min_incoming_ticket_win_prob FROM chain_info WHERE id = 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("select chain_info extras");
        assert_eq!(price_hex, "0000000000000000000000FF");
        assert!((win_prob - 0.25).abs() < f64::EPSILON);
    }

    #[test]
    fn hopr_raw_logs_topics_persist() {
        let db = HoprEventsDb::open_in_memory().expect("in-memory db");
        let topics = vec![B256::from([1u8; 32]), B256::from([2u8; 32])];
        db.record_raw_log(7, 0, 0, Address::ZERO, &topics, b"data", "Evt")
            .expect("insert raw log with topics");
        let (t0, t1): (String, String) = db
            .connection()
            .query_row(
                "SELECT hex(topic0), hex(topic1) FROM hopr_raw_logs WHERE block_number = 7",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("select topics");
        let exp0 = "01".repeat(32);
        let exp1 = "02".repeat(32);
        assert_eq!(t0, exp0);
        assert_eq!(t1, exp1);
    }
}
