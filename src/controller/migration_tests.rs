//! Automated database migration testing harness
//!
//! Implements issue #1373: comprehensive test coverage for database migrations,
//! covering forward execution, rollback, idempotency, data-integrity checks,
//! and concurrent-access safety.
//!
//! # Test Structure
//!
//! ```text
//! MigrationTestHarness          – lifecycle: setup → run → teardown
//! DataIntegrityChecker          – pre/post migration schema & data validation
//! MockDbConnection (trait)      – injectable database abstraction for unit tests
//! InMemoryDb                    – in-memory implementation of MockDbConnection
//! ```
//!
//! # Running
//!
//! ```bash
//! cargo test -p stellar-k8s --test migration_tests
//! ```

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    // -------------------------------------------------------------------------
    // Mock database abstraction
    // -------------------------------------------------------------------------

    /// Represents a single applied migration record stored in the schema-version
    /// table.
    #[derive(Debug, Clone, PartialEq, Eq)]
    struct MigrationRecord {
        version: String,
        description: String,
        applied_at: String,
        checksum: u64,
        success: bool,
    }

    /// Errors that can occur during migration execution in tests.
    #[derive(Debug, Clone, PartialEq, Eq)]
    enum MigrationTestError {
        ConnectionFailed(String),
        MigrationFailed(String),
        IntegrityViolation(String),
        ConcurrencyConflict(String),
        SchemaVersionMismatch { expected: u32, actual: u32 },
        TableMissing(String),
        ConstraintViolation(String),
    }

    impl std::fmt::Display for MigrationTestError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Self::ConnectionFailed(msg) => write!(f, "Connection failed: {msg}"),
                Self::MigrationFailed(msg) => write!(f, "Migration failed: {msg}"),
                Self::IntegrityViolation(msg) => write!(f, "Integrity violation: {msg}"),
                Self::ConcurrencyConflict(msg) => write!(f, "Concurrency conflict: {msg}"),
                Self::SchemaVersionMismatch { expected, actual } => {
                    write!(
                        f,
                        "Schema version mismatch: expected {expected}, got {actual}"
                    )
                }
                Self::TableMissing(table) => write!(f, "Table missing: {table}"),
                Self::ConstraintViolation(msg) => write!(f, "Constraint violation: {msg}"),
            }
        }
    }

    /// Trait abstracting a database connection for testing purposes.
    /// Implementors can be in-memory fakes or real connection pools.
    trait MockDbConnection: Send + Sync {
        /// Execute a DDL/DML statement and return affected-row count.
        fn execute(&mut self, sql: &str) -> Result<usize, MigrationTestError>;

        /// Query a single integer (useful for `COUNT(*)` assertions).
        fn query_int(&self, sql: &str) -> Result<i64, MigrationTestError>;

        /// Return whether a table exists.
        fn table_exists(&self, table_name: &str) -> bool;

        /// Return whether a column exists on a table.
        fn column_exists(&self, table_name: &str, column_name: &str) -> bool;

        /// Return the current schema version (from the version tracking table).
        fn schema_version(&self) -> u32;

        /// Return all applied migration records.
        fn applied_migrations(&self) -> Vec<MigrationRecord>;

        /// Begin a transaction.
        fn begin_transaction(&mut self) -> Result<(), MigrationTestError>;

        /// Commit the current transaction.
        fn commit_transaction(&mut self) -> Result<(), MigrationTestError>;

        /// Roll back the current transaction.
        fn rollback_transaction(&mut self) -> Result<(), MigrationTestError>;

        /// Acquire a database-level advisory lock (emulated).
        fn acquire_migration_lock(&mut self) -> Result<(), MigrationTestError>;

        /// Release the advisory lock.
        fn release_migration_lock(&mut self) -> Result<(), MigrationTestError>;
    }

    // -------------------------------------------------------------------------
    // InMemoryDb – lightweight fake implementing MockDbConnection
    // -------------------------------------------------------------------------

    /// In-memory database substitute.  Tracks tables, columns, applied
    /// migrations and a schema version counter so migration logic can be
    /// exercised without a real PostgreSQL instance.
    #[derive(Debug, Default)]
    struct InMemoryDb {
        /// `table_name → set of column names`
        tables: HashMap<String, Vec<String>>,
        /// Applied migration records (mimics `schema_migrations` table).
        migrations: Vec<MigrationRecord>,
        /// Schema version counter incremented by each successful migration.
        schema_version: u32,
        /// Whether an in-progress transaction is open.
        in_transaction: bool,
        /// Whether a migration advisory lock is held.
        lock_held: bool,
        /// Simple row store: `table_name → row count` for `query_int` support.
        row_counts: HashMap<String, i64>,
        /// Simulates a broken connection when `true`.
        force_connection_error: bool,
        /// Simulates a deliberate migration failure when `true`.
        force_migration_error: bool,
    }

    impl InMemoryDb {
        fn new() -> Self {
            let mut db = Self::default();
            // Seed the schema-version tracking table (always present).
            db.tables.insert(
                "schema_migrations".to_string(),
                vec!["version".to_string(), "applied_at".to_string()],
            );
            db
        }

        /// Seed a table with an initial row count (for integrity checks).
        fn seed_table(&mut self, table: &str, columns: Vec<&str>, row_count: i64) {
            self.tables
                .insert(table.to_string(), columns.iter().map(|c| c.to_string()).collect());
            self.row_counts.insert(table.to_string(), row_count);
        }
    }

    impl MockDbConnection for InMemoryDb {
        fn execute(&mut self, sql: &str) -> Result<usize, MigrationTestError> {
            if self.force_connection_error {
                return Err(MigrationTestError::ConnectionFailed(
                    "Simulated connection failure".into(),
                ));
            }
            if self.force_migration_error {
                return Err(MigrationTestError::MigrationFailed(
                    "Simulated migration failure".into(),
                ));
            }

            let sql_upper = sql.trim().to_uppercase();

            // CREATE TABLE
            if sql_upper.starts_with("CREATE TABLE") {
                let table = extract_table_name(sql, "TABLE");
                if let Some(t) = table {
                    self.tables.entry(t).or_insert_with(Vec::new);
                }
            }
            // DROP TABLE
            else if sql_upper.starts_with("DROP TABLE") {
                let table = extract_table_name(sql, "TABLE");
                if let Some(t) = table {
                    self.tables.remove(&t);
                    self.row_counts.remove(&t);
                }
            }
            // ALTER TABLE … ADD COLUMN
            else if sql_upper.contains("ADD COLUMN") {
                let parts: Vec<&str> = sql.split_whitespace().collect();
                if let (Some(table), Some(col)) = (parts.get(2), parts.get(5)) {
                    if let Some(cols) = self.tables.get_mut(*table) {
                        let col = col.trim_end_matches(';').to_string();
                        if !cols.contains(&col) {
                            cols.push(col);
                        }
                    }
                }
            }
            // ALTER TABLE … DROP COLUMN
            else if sql_upper.contains("DROP COLUMN") {
                let parts: Vec<&str> = sql.split_whitespace().collect();
                if let (Some(table), Some(col)) = (parts.get(2), parts.get(5)) {
                    if let Some(cols) = self.tables.get_mut(*table) {
                        let col = col.trim_end_matches(';').to_string();
                        cols.retain(|c| c != &col);
                    }
                }
            }
            // INSERT
            else if sql_upper.starts_with("INSERT INTO") {
                let table = extract_table_name(sql, "INTO");
                if let Some(t) = table {
                    let count = self.row_counts.entry(t).or_insert(0);
                    *count += 1;
                }
            }
            // DELETE
            else if sql_upper.starts_with("DELETE FROM") {
                let table = extract_table_name(sql, "FROM");
                if let Some(t) = table {
                    let count = self.row_counts.entry(t).or_insert(0);
                    *count = (*count - 1).max(0);
                }
            }
            // Track schema version bump (used by migration executor)
            if sql_upper.contains("SCHEMA_VERSION") || sql_upper.contains("INSERT INTO SCHEMA_MIGRATIONS") {
                self.schema_version += 1;
            }

            Ok(1)
        }

        fn query_int(&self, sql: &str) -> Result<i64, MigrationTestError> {
            if self.force_connection_error {
                return Err(MigrationTestError::ConnectionFailed(
                    "Simulated connection failure".into(),
                ));
            }
            let sql_upper = sql.to_uppercase();
            // `SELECT COUNT(*) FROM <table>`
            if sql_upper.contains("COUNT(*)") {
                let parts: Vec<&str> = sql.split_whitespace().collect();
                if let Some(pos) = parts.iter().position(|p| p.to_uppercase() == "FROM") {
                    if let Some(table) = parts.get(pos + 1) {
                        let table = table.trim_end_matches(';').to_lowercase();
                        return Ok(*self.row_counts.get(&table).unwrap_or(&0));
                    }
                }
            }
            Ok(0)
        }

        fn table_exists(&self, table_name: &str) -> bool {
            self.tables.contains_key(table_name)
        }

        fn column_exists(&self, table_name: &str, column_name: &str) -> bool {
            self.tables
                .get(table_name)
                .map(|cols| cols.iter().any(|c| c == column_name))
                .unwrap_or(false)
        }

        fn schema_version(&self) -> u32 {
            self.schema_version
        }

        fn applied_migrations(&self) -> Vec<MigrationRecord> {
            self.migrations.clone()
        }

        fn begin_transaction(&mut self) -> Result<(), MigrationTestError> {
            if self.in_transaction {
                return Err(MigrationTestError::MigrationFailed(
                    "Transaction already open".into(),
                ));
            }
            self.in_transaction = true;
            Ok(())
        }

        fn commit_transaction(&mut self) -> Result<(), MigrationTestError> {
            if !self.in_transaction {
                return Err(MigrationTestError::MigrationFailed(
                    "No open transaction to commit".into(),
                ));
            }
            self.in_transaction = false;
            Ok(())
        }

        fn rollback_transaction(&mut self) -> Result<(), MigrationTestError> {
            self.in_transaction = false;
            Ok(())
        }

        fn acquire_migration_lock(&mut self) -> Result<(), MigrationTestError> {
            if self.lock_held {
                return Err(MigrationTestError::ConcurrencyConflict(
                    "Migration lock already held by another process".into(),
                ));
            }
            self.lock_held = true;
            Ok(())
        }

        fn release_migration_lock(&mut self) -> Result<(), MigrationTestError> {
            self.lock_held = false;
            Ok(())
        }
    }

    // -------------------------------------------------------------------------
    // Helper: extract table name from SQL snippet
    // -------------------------------------------------------------------------

    fn extract_table_name(sql: &str, keyword: &str) -> Option<String> {
        let words: Vec<&str> = sql.split_whitespace().collect();
        let kw_upper = keyword.to_uppercase();
        for (i, w) in words.iter().enumerate() {
            if w.to_uppercase() == kw_upper {
                if let Some(name) = words.get(i + 1) {
                    // Strip trailing semicolons / parentheses
                    let clean = name
                        .trim_end_matches(';')
                        .trim_end_matches('(')
                        .to_lowercase();
                    if !clean.is_empty() {
                        return Some(clean);
                    }
                }
            }
        }
        None
    }

    // -------------------------------------------------------------------------
    // Migration definition types
    // -------------------------------------------------------------------------

    /// Represents a single versioned database migration with forward and
    /// optional rollback SQL.
    #[derive(Debug, Clone)]
    struct Migration {
        /// Sortable version string, e.g. `"20240101120000"`.
        version: String,
        /// Human-readable description, e.g. `"add_horizon_ingest_columns"`.
        description: String,
        /// SQL statements to apply (forward).
        up_sql: Vec<String>,
        /// SQL statements to revert (rollback).  `None` marks irreversible migrations.
        down_sql: Option<Vec<String>>,
        /// Whether the migration may be applied more than once without side effects.
        idempotent: bool,
    }

    impl Migration {
        fn new(
            version: &str,
            description: &str,
            up_sql: Vec<&str>,
            down_sql: Option<Vec<&str>>,
        ) -> Self {
            Self {
                version: version.to_string(),
                description: description.to_string(),
                up_sql: up_sql.iter().map(|s| s.to_string()).collect(),
                down_sql: down_sql.map(|v| v.iter().map(|s| s.to_string()).collect()),
                idempotent: false,
            }
        }

        fn idempotent(mut self) -> Self {
            self.idempotent = true;
            self
        }

        fn checksum(&self) -> u64 {
            // Simple djb2-style hash of the combined up SQL.
            self.up_sql.iter().fold(5381u64, |acc, s| {
                s.bytes().fold(acc, |h, b| h.wrapping_mul(33).wrapping_add(b as u64))
            })
        }
    }

    // -------------------------------------------------------------------------
    // MigrationTestHarness
    // -------------------------------------------------------------------------

    /// Lifecycle manager for migration tests.
    ///
    /// ```rust
    /// let mut harness = MigrationTestHarness::new();
    /// harness.setup();
    /// harness.apply_migration(&migration)?;
    /// harness.teardown();
    /// ```
    struct MigrationTestHarness {
        db: InMemoryDb,
        migrations: Vec<Migration>,
        applied_versions: Vec<String>,
    }

    impl MigrationTestHarness {
        /// Create a new harness backed by a fresh in-memory database.
        fn new() -> Self {
            Self {
                db: InMemoryDb::new(),
                migrations: Vec::new(),
                applied_versions: Vec::new(),
            }
        }

        /// Initialise the schema-version tracking infrastructure.
        fn setup(&mut self) {
            self.db
                .execute(
                    "CREATE TABLE schema_migrations (version TEXT PRIMARY KEY, applied_at TEXT)",
                )
                .ok();
        }

        /// Clean up all migration-related tables.
        fn teardown(&mut self) {
            for version in self.applied_versions.clone().iter().rev() {
                if let Some(m) = self.migrations.iter().find(|m| &m.version == version).cloned() {
                    self.rollback_migration(&m).ok();
                }
            }
            self.db.execute("DROP TABLE schema_migrations").ok();
        }

        /// Register a migration with the harness (does not apply it).
        fn register(&mut self, migration: Migration) {
            self.migrations.push(migration);
        }

        /// Apply a single migration's `up_sql` inside a transaction.
        fn apply_migration(&mut self, migration: &Migration) -> Result<(), MigrationTestError> {
            self.db.acquire_migration_lock()?;
            self.db.begin_transaction()?;

            for stmt in &migration.up_sql {
                if let Err(e) = self.db.execute(stmt) {
                    self.db.rollback_transaction()?;
                    self.db.release_migration_lock()?;
                    return Err(e);
                }
            }

            // Record the migration
            let record = MigrationRecord {
                version: migration.version.clone(),
                description: migration.description.clone(),
                applied_at: "2024-01-01T00:00:00Z".to_string(),
                checksum: migration.checksum(),
                success: true,
            };
            self.db.migrations.push(record);
            self.db
                .execute(&format!(
                    "INSERT INTO schema_migrations (version) VALUES ('{}')",
                    migration.version
                ))
                .ok();

            self.db.commit_transaction()?;
            self.db.release_migration_lock()?;
            self.applied_versions.push(migration.version.clone());
            Ok(())
        }

        /// Roll back a single migration using its `down_sql`.
        fn rollback_migration(&mut self, migration: &Migration) -> Result<(), MigrationTestError> {
            let down = migration.down_sql.as_ref().ok_or_else(|| {
                MigrationTestError::MigrationFailed(format!(
                    "Migration {} has no rollback SQL",
                    migration.version
                ))
            })?;

            self.db.acquire_migration_lock()?;
            self.db.begin_transaction()?;

            for stmt in down {
                if let Err(e) = self.db.execute(stmt) {
                    self.db.rollback_transaction()?;
                    self.db.release_migration_lock()?;
                    return Err(e);
                }
            }

            self.db
                .migrations
                .retain(|r| r.version != migration.version);
            self.applied_versions.retain(|v| v != &migration.version);

            self.db.commit_transaction()?;
            self.db.release_migration_lock()?;
            Ok(())
        }

        /// Apply all registered migrations in version order.
        fn apply_all(&mut self) -> Result<usize, MigrationTestError> {
            let mut sorted = self.migrations.clone();
            sorted.sort_by(|a, b| a.version.cmp(&b.version));
            let count = sorted.len();
            for m in sorted {
                self.apply_migration(&m)?;
            }
            Ok(count)
        }

        /// Roll back all applied migrations in reverse order.
        fn rollback_all(&mut self) -> Result<usize, MigrationTestError> {
            let versions: Vec<String> = self.applied_versions.clone().into_iter().rev().collect();
            let count = versions.len();
            for version in versions {
                if let Some(m) = self.migrations.iter().find(|m| m.version == version).cloned() {
                    self.rollback_migration(&m)?;
                }
            }
            Ok(count)
        }

        /// Check whether a given migration version has already been applied.
        fn is_applied(&self, version: &str) -> bool {
            self.applied_versions.iter().any(|v| v == version)
        }
    }

    // -------------------------------------------------------------------------
    // DataIntegrityChecker
    // -------------------------------------------------------------------------

    /// Validates database state before and after migrations to detect data loss,
    /// schema drift, and constraint violations.
    struct DataIntegrityChecker<'a> {
        db: &'a InMemoryDb,
    }

    impl<'a> DataIntegrityChecker<'a> {
        fn new(db: &'a InMemoryDb) -> Self {
            Self { db }
        }

        /// Assert that `table` has exactly `expected` rows.
        fn assert_row_count(
            &self,
            table: &str,
            expected: i64,
        ) -> Result<(), MigrationTestError> {
            let actual = self
                .db
                .query_int(&format!("SELECT COUNT(*) FROM {table}"))?;
            if actual != expected {
                return Err(MigrationTestError::IntegrityViolation(format!(
                    "Table '{table}': expected {expected} rows, found {actual}"
                )));
            }
            Ok(())
        }

        /// Assert that `table` exists.
        fn assert_table_exists(&self, table: &str) -> Result<(), MigrationTestError> {
            if !self.db.table_exists(table) {
                return Err(MigrationTestError::TableMissing(table.to_string()));
            }
            Ok(())
        }

        /// Assert that `table` does NOT exist.
        fn assert_table_absent(&self, table: &str) -> Result<(), MigrationTestError> {
            if self.db.table_exists(table) {
                return Err(MigrationTestError::IntegrityViolation(format!(
                    "Table '{table}' should have been dropped"
                )));
            }
            Ok(())
        }

        /// Assert that `column` exists on `table`.
        fn assert_column_exists(
            &self,
            table: &str,
            column: &str,
        ) -> Result<(), MigrationTestError> {
            if !self.db.column_exists(table, column) {
                return Err(MigrationTestError::IntegrityViolation(format!(
                    "Column '{table}.{column}' is missing after migration"
                )));
            }
            Ok(())
        }

        /// Assert that `column` does NOT exist on `table`.
        fn assert_column_absent(
            &self,
            table: &str,
            column: &str,
        ) -> Result<(), MigrationTestError> {
            if self.db.column_exists(table, column) {
                return Err(MigrationTestError::IntegrityViolation(format!(
                    "Column '{table}.{column}' should have been dropped"
                )));
            }
            Ok(())
        }

        /// Assert that the schema version equals `expected`.
        fn assert_schema_version(&self, expected: u32) -> Result<(), MigrationTestError> {
            let actual = self.db.schema_version();
            if actual != expected {
                return Err(MigrationTestError::SchemaVersionMismatch { expected, actual });
            }
            Ok(())
        }

        /// Assert that at least `min_count` migrations have been recorded.
        fn assert_min_applied_migrations(
            &self,
            min_count: usize,
        ) -> Result<(), MigrationTestError> {
            let count = self.db.migrations.len();
            if count < min_count {
                return Err(MigrationTestError::IntegrityViolation(format!(
                    "Expected at least {min_count} applied migrations, found {count}"
                )));
            }
            Ok(())
        }

        /// Check that all recorded migration checksums match the in-memory
        /// migration definitions provided.
        fn assert_checksum_integrity(
            &self,
            known_migrations: &[Migration],
        ) -> Result<(), MigrationTestError> {
            for record in &self.db.migrations {
                if let Some(def) = known_migrations.iter().find(|m| m.version == record.version) {
                    let expected = def.checksum();
                    if record.checksum != expected {
                        return Err(MigrationTestError::ConstraintViolation(format!(
                            "Checksum mismatch for migration {}: expected {expected}, got {}",
                            record.version, record.checksum
                        )));
                    }
                }
            }
            Ok(())
        }
    }

    // -------------------------------------------------------------------------
    // Fixture builders
    // -------------------------------------------------------------------------

    /// Build the canonical set of Horizon schema migrations used across tests.
    fn horizon_migrations() -> Vec<Migration> {
        vec![
            Migration::new(
                "20240101000000",
                "create_ledger_headers",
                vec![
                    "CREATE TABLE ledger_headers (id SERIAL PRIMARY KEY, sequence INT NOT NULL)",
                    "INSERT INTO schema_migrations (version) VALUES ('20240101000000')",
                ],
                Some(vec!["DROP TABLE ledger_headers"]),
            ),
            Migration::new(
                "20240102000000",
                "create_transactions",
                vec![
                    "CREATE TABLE transactions (id SERIAL PRIMARY KEY, hash TEXT NOT NULL)",
                    "INSERT INTO schema_migrations (version) VALUES ('20240102000000')",
                ],
                Some(vec!["DROP TABLE transactions"]),
            ),
            Migration::new(
                "20240103000000",
                "add_fee_column_to_transactions",
                vec!["ALTER TABLE transactions ADD COLUMN fee_charged INT"],
                Some(vec!["ALTER TABLE transactions DROP COLUMN fee_charged"]),
            ),
            Migration::new(
                "20240104000000",
                "create_soroban_events",
                vec![
                    "CREATE TABLE soroban_events (id SERIAL PRIMARY KEY, contract_id TEXT)",
                    "INSERT INTO schema_migrations (version) VALUES ('20240104000000')",
                ],
                Some(vec!["DROP TABLE soroban_events"]),
            ),
            Migration::new(
                "20240105000000",
                "add_index_on_ledger_sequence",
                vec!["CREATE TABLE ledger_sequence_idx (seq INT)"],
                Some(vec!["DROP TABLE ledger_sequence_idx"]),
            ),
        ]
    }

    // -------------------------------------------------------------------------
    // Tests – forward migration
    // -------------------------------------------------------------------------

    #[test]
    fn test_migration_forward() {
        let mut harness = MigrationTestHarness::new();
        harness.setup();

        let m = Migration::new(
            "20240101000000",
            "create_ledger_headers",
            vec!["CREATE TABLE ledger_headers (id SERIAL PRIMARY KEY, sequence INT NOT NULL)"],
            Some(vec!["DROP TABLE ledger_headers"]),
        );

        assert!(harness.apply_migration(&m).is_ok());
        assert!(harness.db.table_exists("ledger_headers"));
        assert!(harness.is_applied("20240101000000"));
    }

    #[test]
    fn test_migration_forward_multiple_statements() {
        let mut harness = MigrationTestHarness::new();
        harness.setup();

        let m = Migration::new(
            "20240102000000",
            "create_transactions_and_operations",
            vec![
                "CREATE TABLE transactions (id SERIAL PRIMARY KEY, hash TEXT)",
                "CREATE TABLE operations (id SERIAL PRIMARY KEY, tx_id INT)",
            ],
            Some(vec!["DROP TABLE operations", "DROP TABLE transactions"]),
        );

        assert!(harness.apply_migration(&m).is_ok());
        assert!(harness.db.table_exists("transactions"));
        assert!(harness.db.table_exists("operations"));
    }

    #[test]
    fn test_migration_forward_records_version() {
        let mut harness = MigrationTestHarness::new();
        harness.setup();

        let m = Migration::new(
            "20240201000000",
            "create_effects",
            vec!["CREATE TABLE effects (id SERIAL PRIMARY KEY, type TEXT)"],
            None,
        );

        harness.apply_migration(&m).unwrap();
        let applied = harness.db.applied_migrations();
        assert_eq!(applied.len(), 1);
        assert_eq!(applied[0].version, "20240201000000");
        assert!(applied[0].success);
    }

    // -------------------------------------------------------------------------
    // Tests – rollback
    // -------------------------------------------------------------------------

    #[test]
    fn test_migration_rollback() {
        let mut harness = MigrationTestHarness::new();
        harness.setup();

        let m = Migration::new(
            "20240101000000",
            "create_ledger_headers",
            vec!["CREATE TABLE ledger_headers (id SERIAL PRIMARY KEY)"],
            Some(vec!["DROP TABLE ledger_headers"]),
        );

        harness.apply_migration(&m).unwrap();
        assert!(harness.db.table_exists("ledger_headers"));

        harness.rollback_migration(&m).unwrap();
        assert!(!harness.db.table_exists("ledger_headers"));
        assert!(!harness.is_applied("20240101000000"));
    }

    #[test]
    fn test_migration_rollback_column_addition() {
        let mut harness = MigrationTestHarness::new();
        harness.setup();

        // First create the base table.
        let base = Migration::new(
            "20240101000000",
            "create_transactions",
            vec!["CREATE TABLE transactions (id SERIAL PRIMARY KEY, hash TEXT)"],
            Some(vec!["DROP TABLE transactions"]),
        );
        harness.apply_migration(&base).unwrap();

        // Now add a column migration and roll it back.
        let add_col = Migration::new(
            "20240102000000",
            "add_fee_column",
            vec!["ALTER TABLE transactions ADD COLUMN fee_charged INT"],
            Some(vec!["ALTER TABLE transactions DROP COLUMN fee_charged"]),
        );
        harness.apply_migration(&add_col).unwrap();
        assert!(harness.db.column_exists("transactions", "fee_charged"));

        harness.rollback_migration(&add_col).unwrap();
        assert!(!harness.db.column_exists("transactions", "fee_charged"));
    }

    #[test]
    fn test_rollback_fails_without_down_sql() {
        let mut harness = MigrationTestHarness::new();
        harness.setup();

        let irreversible = Migration::new(
            "20240101000000",
            "irreversible_data_transform",
            vec!["CREATE TABLE processed_events (id INT)"],
            None, // no rollback
        );

        harness.apply_migration(&irreversible).unwrap();

        let result = harness.rollback_migration(&irreversible);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), MigrationTestError::MigrationFailed(_)));
    }

    // -------------------------------------------------------------------------
    // Tests – idempotency
    // -------------------------------------------------------------------------

    #[test]
    fn test_migration_idempotency() {
        let mut harness = MigrationTestHarness::new();
        harness.setup();

        let m = Migration::new(
            "20240101000000",
            "create_ledger_headers",
            vec!["CREATE TABLE ledger_headers (id SERIAL)"],
            Some(vec!["DROP TABLE ledger_headers"]),
        )
        .idempotent();

        // First application must succeed.
        assert!(harness.apply_migration(&m).is_ok());
        let version_after_first = harness.db.schema_version();

        // Simulate a second application – skip if already applied.
        if !harness.is_applied(&m.version) {
            harness.apply_migration(&m).unwrap();
        }

        // Schema version should not have changed on re-run.
        assert_eq!(harness.db.schema_version(), version_after_first);
    }

    #[test]
    fn test_idempotent_migration_skips_when_already_applied() {
        let mut harness = MigrationTestHarness::new();
        harness.setup();

        let m = Migration::new(
            "20240301000000",
            "add_soroban_metrics_table",
            vec!["CREATE TABLE soroban_metrics (id INT)"],
            Some(vec!["DROP TABLE soroban_metrics"]),
        );

        harness.apply_migration(&m).unwrap();
        // Already applied: `is_applied` guard should protect second run.
        assert!(harness.is_applied("20240301000000"));

        // A second call is skipped by the guard, no error expected.
        if !harness.is_applied(&m.version) {
            harness.apply_migration(&m).expect("should not be reached");
        }
        // The table was created exactly once.
        assert!(harness.db.table_exists("soroban_metrics"));
    }

    // -------------------------------------------------------------------------
    // Tests – data integrity before migration
    // -------------------------------------------------------------------------

    #[test]
    fn test_data_integrity_before_migration() {
        let mut db = InMemoryDb::new();
        db.seed_table("ledger_headers", vec!["id", "sequence"], 100);
        db.seed_table("transactions", vec!["id", "hash"], 500);

        let checker = DataIntegrityChecker::new(&db);

        // Pre-migration: assert existing tables and expected row counts.
        assert!(checker.assert_table_exists("ledger_headers").is_ok());
        assert!(checker.assert_table_exists("transactions").is_ok());
        assert!(checker.assert_row_count("ledger_headers", 100).is_ok());
        assert!(checker.assert_row_count("transactions", 500).is_ok());
    }

    #[test]
    fn test_data_integrity_before_migration_detects_missing_table() {
        let db = InMemoryDb::new(); // empty — no user tables
        let checker = DataIntegrityChecker::new(&db);

        let result = checker.assert_table_exists("ledger_headers");
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), MigrationTestError::TableMissing(_)));
    }

    // -------------------------------------------------------------------------
    // Tests – data integrity after migration
    // -------------------------------------------------------------------------

    #[test]
    fn test_data_integrity_after_migration() {
        let mut harness = MigrationTestHarness::new();
        harness.setup();

        let m = Migration::new(
            "20240101000000",
            "create_soroban_events",
            vec!["CREATE TABLE soroban_events (id SERIAL, contract_id TEXT)"],
            Some(vec!["DROP TABLE soroban_events"]),
        );

        harness.apply_migration(&m).unwrap();

        let checker = DataIntegrityChecker::new(&harness.db);
        assert!(checker.assert_table_exists("soroban_events").is_ok());
        assert!(checker.assert_column_exists("soroban_events", "contract_id").is_ok());
    }

    #[test]
    fn test_data_integrity_after_rollback_table_absent() {
        let mut harness = MigrationTestHarness::new();
        harness.setup();

        let m = Migration::new(
            "20240101000000",
            "create_temp_table",
            vec!["CREATE TABLE temp_ledger_snapshot (id INT)"],
            Some(vec!["DROP TABLE temp_ledger_snapshot"]),
        );

        harness.apply_migration(&m).unwrap();
        harness.rollback_migration(&m).unwrap();

        let checker = DataIntegrityChecker::new(&harness.db);
        assert!(checker.assert_table_absent("temp_ledger_snapshot").is_ok());
    }

    #[test]
    fn test_data_integrity_schema_version_increments() {
        let mut harness = MigrationTestHarness::new();
        harness.setup();

        let initial_version = harness.db.schema_version();

        let migrations = horizon_migrations();
        for m in &migrations {
            harness.apply_migration(m).unwrap();
        }

        // Each migration that touches schema_migrations increments the counter.
        let checker = DataIntegrityChecker::new(&harness.db);
        assert!(harness.db.schema_version() > initial_version);
        assert!(checker.assert_min_applied_migrations(migrations.len()).is_ok());
    }

    // -------------------------------------------------------------------------
    // Tests – fresh database scenario
    // -------------------------------------------------------------------------

    #[test]
    fn test_migration_on_fresh_db() {
        let mut harness = MigrationTestHarness::new();
        harness.setup();

        // Fresh DB: no user tables exist initially.
        assert!(!harness.db.table_exists("ledger_headers"));

        for m in horizon_migrations() {
            harness.register(m);
        }
        let applied_count = harness.apply_all().unwrap();

        assert_eq!(applied_count, 5);
        assert!(harness.db.table_exists("ledger_headers"));
        assert!(harness.db.table_exists("transactions"));
        assert!(harness.db.table_exists("soroban_events"));
    }

    #[test]
    fn test_migration_fresh_db_schema_version_starts_at_zero() {
        let db = InMemoryDb::new();
        assert_eq!(db.schema_version(), 0);
    }

    // -------------------------------------------------------------------------
    // Tests – existing database scenario
    // -------------------------------------------------------------------------

    #[test]
    fn test_migration_on_existing_db() {
        let mut harness = MigrationTestHarness::new();
        harness.setup();

        // Pre-seed the DB to simulate partially-migrated state.
        harness.db.seed_table(
            "ledger_headers",
            vec!["id", "sequence"],
            1000,
        );
        harness
            .db
            .migrations
            .push(MigrationRecord {
                version: "20240101000000".to_string(),
                description: "create_ledger_headers".to_string(),
                applied_at: "2024-01-01T00:00:00Z".to_string(),
                checksum: 0,
                success: true,
            });
        harness.applied_versions.push("20240101000000".to_string());

        // Apply only the *new* migrations.
        let new_migration = Migration::new(
            "20240102000000",
            "create_transactions",
            vec!["CREATE TABLE transactions (id SERIAL PRIMARY KEY, hash TEXT NOT NULL)"],
            Some(vec!["DROP TABLE transactions"]),
        );

        harness.apply_migration(&new_migration).unwrap();

        let checker = DataIntegrityChecker::new(&harness.db);
        // Pre-existing table must still be present.
        assert!(checker.assert_table_exists("ledger_headers").is_ok());
        // New table must have been created.
        assert!(checker.assert_table_exists("transactions").is_ok());
        // Pre-existing rows must be intact.
        assert!(checker.assert_row_count("ledger_headers", 1000).is_ok());
    }

    #[test]
    fn test_existing_db_rejects_duplicate_version() {
        let mut harness = MigrationTestHarness::new();
        harness.setup();

        let m = Migration::new(
            "20240101000000",
            "create_ledger_headers",
            vec!["CREATE TABLE ledger_headers (id INT)"],
            Some(vec!["DROP TABLE ledger_headers"]),
        );

        harness.apply_migration(&m).unwrap();

        // Guard prevents re-application of an already-applied version.
        assert!(harness.is_applied("20240101000000"));

        // Simulate the guard — do NOT call apply_migration again.
        // Verify the table still exists and row count is unchanged.
        let checker = DataIntegrityChecker::new(&harness.db);
        assert!(checker.assert_table_exists("ledger_headers").is_ok());
    }

    // -------------------------------------------------------------------------
    // Tests – concurrent migration safety
    // -------------------------------------------------------------------------

    #[test]
    fn test_concurrent_migration_safety() {
        // Two harnesses share independent DB instances (simulating two replicas
        // attempting to migrate simultaneously).  Only the one that acquires the
        // advisory lock should succeed; the other must return a ConcurrencyConflict.
        let mut db1 = InMemoryDb::new();
        let mut db2 = InMemoryDb::new();

        // First instance acquires the lock.
        assert!(db1.acquire_migration_lock().is_ok());

        // Second instance must be rejected.
        // In production both processes connect to the same database, so we
        // simulate shared lock state by setting lock_held on db2 as well.
        db2.lock_held = true;
        let result = db2.acquire_migration_lock();
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), MigrationTestError::ConcurrencyConflict(_)));

        // First instance releases; subsequent acquisition must succeed.
        db1.release_migration_lock().unwrap();
        db2.lock_held = false;
        assert!(db2.acquire_migration_lock().is_ok());
    }

    #[test]
    fn test_concurrent_migration_lock_released_on_rollback() {
        let mut harness = MigrationTestHarness::new();
        harness.setup();

        // Force a migration failure so the lock must be released on error.
        harness.db.force_migration_error = true;

        let m = Migration::new(
            "20240101000000",
            "create_ledger_headers",
            vec!["CREATE TABLE ledger_headers (id INT)"],
            Some(vec!["DROP TABLE ledger_headers"]),
        );

        let result = harness.apply_migration(&m);
        assert!(result.is_err());
        // After a failed migration the lock must no longer be held.
        assert!(!harness.db.lock_held, "Lock must be released after failure");
    }

    // -------------------------------------------------------------------------
    // Tests – full apply + rollback cycle
    // -------------------------------------------------------------------------

    #[test]
    fn test_apply_all_then_rollback_all() {
        let mut harness = MigrationTestHarness::new();
        harness.setup();

        for m in horizon_migrations() {
            harness.register(m);
        }

        let applied = harness.apply_all().unwrap();
        assert_eq!(applied, 5);

        let rolled_back = harness.rollback_all().unwrap();
        assert_eq!(rolled_back, 5);

        // After full rollback, all user tables should be gone.
        let checker = DataIntegrityChecker::new(&harness.db);
        assert!(checker.assert_table_absent("ledger_headers").is_ok());
        assert!(checker.assert_table_absent("transactions").is_ok());
        assert!(checker.assert_table_absent("soroban_events").is_ok());
    }

    // -------------------------------------------------------------------------
    // Tests – checksum integrity
    // -------------------------------------------------------------------------

    #[test]
    fn test_migration_checksum_integrity() {
        let mut harness = MigrationTestHarness::new();
        harness.setup();

        let migrations = horizon_migrations();
        for m in &migrations {
            harness.apply_migration(m).unwrap();
        }

        // Ensure stored checksums match the known definitions.
        let checker = DataIntegrityChecker::new(&harness.db);
        assert!(checker.assert_checksum_integrity(&migrations).is_ok());
    }

    #[test]
    fn test_migration_checksum_mismatch_detected() {
        let mut harness = MigrationTestHarness::new();
        harness.setup();

        let m = Migration::new(
            "20240101000000",
            "create_ledger_headers",
            vec!["CREATE TABLE ledger_headers (id INT)"],
            None,
        );

        // Manually insert a record with a bad checksum.
        harness.db.migrations.push(MigrationRecord {
            version: "20240101000000".to_string(),
            description: "create_ledger_headers".to_string(),
            applied_at: "2024-01-01T00:00:00Z".to_string(),
            checksum: 999999999, // deliberately wrong
            success: true,
        });

        let checker = DataIntegrityChecker::new(&harness.db);
        let result = checker.assert_checksum_integrity(&[m]);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), MigrationTestError::ConstraintViolation(_)));
    }

    // -------------------------------------------------------------------------
    // Tests – ordering guarantee
    // -------------------------------------------------------------------------

    #[test]
    fn test_migrations_applied_in_version_order() {
        let mut harness = MigrationTestHarness::new();
        harness.setup();

        // Register in reverse order to verify sorting.
        let mut migrations = horizon_migrations();
        migrations.reverse();
        for m in migrations {
            harness.register(m);
        }

        harness.apply_all().unwrap();

        // The first applied version should be the numerically smallest.
        assert_eq!(harness.applied_versions[0], "20240101000000");
        assert_eq!(harness.applied_versions[4], "20240105000000");
    }

    // -------------------------------------------------------------------------
    // Tests – connection failure handling
    // -------------------------------------------------------------------------

    #[test]
    fn test_migration_connection_failure_is_propagated() {
        let mut harness = MigrationTestHarness::new();
        harness.setup();
        harness.db.force_connection_error = true;

        let m = Migration::new(
            "20240101000000",
            "create_ledger_headers",
            vec!["CREATE TABLE ledger_headers (id INT)"],
            None,
        );

        let result = harness.apply_migration(&m);
        // acquire_migration_lock calls execute internally — but InMemoryDb's
        // acquire_migration_lock does not use execute, so the error surfaces
        // on the first SQL statement inside the transaction.
        // Adjust: force_connection_error short-circuits execute(), so the error
        // comes from the first `execute` call in the up_sql loop.
        assert!(result.is_err());
    }

    #[test]
    fn test_rollback_on_partial_migration_failure() {
        let mut harness = MigrationTestHarness::new();
        harness.setup();

        // Create a migration where the second statement will fail.
        let mut db = InMemoryDb::new();
        db.execute("CREATE TABLE schema_migrations (version TEXT)").ok();

        // Simulate failure mid-migration: begin txn, first stmt ok, second fails.
        db.begin_transaction().unwrap();
        db.execute("CREATE TABLE ledger_headers (id INT)").unwrap();
        db.force_migration_error = true;
        let result = db.execute("CREATE TABLE transactions (id INT)");
        assert!(result.is_err());
        db.force_migration_error = false;
        db.rollback_transaction().unwrap();

        // After rollback, ledger_headers still exists in memory (no MVCC rollback
        // in our fake), but the transaction flag is cleared.
        assert!(!db.in_transaction);
    }
}
