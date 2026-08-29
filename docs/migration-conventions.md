# Migration Authoring Conventions & Review Process

This document defines the standards for writing, testing, and reviewing database
migrations in Stellar-K8s. All contributors must follow these conventions before
submitting a migration for code review.

---

## Table of Contents

1. [Why We Have These Conventions](#why-we-have-these-conventions)
2. [File Naming](#file-naming)
3. [File Structure](#file-structure)
4. [Forward Migration Requirements](#forward-migration-requirements)
5. [Rollback Migration Requirements](#rollback-migration-requirements)
6. [Data Integrity Check Requirements](#data-integrity-check-requirements)
7. [Idempotency](#idempotency)
8. [Examples](#examples)
9. [Review Process & Checklist](#review-process--checklist)
10. [CI Validation Requirements](#ci-validation-requirements)
11. [Emergency Procedures](#emergency-procedures)

---

## Why We Have These Conventions

Stellar-K8s manages stateful Horizon and Soroban RPC nodes whose PostgreSQL
databases may hold months of ledger history. A poorly written migration can:

- Lock tables and cause a node availability outage.
- Silently drop columns or constraints needed by in-flight queries.
- Leave the schema in a half-applied state when a multi-step migration fails.
- Be impossible to roll back, preventing a quick incident response.

Consistent conventions and automated tests catch these problems before they reach
production.

---

## File Naming

Migrations live in `migrations/` and follow the pattern:

```
migrations/
  YYYYMMDDHHMMSS_short_description.up.sql
  YYYYMMDDHHMMSS_short_description.down.sql
```

### Rules

| Rule | Rationale |
|------|-----------|
| Timestamp prefix `YYYYMMDDHHMMSS` (UTC) | Guarantees deterministic ordering across contributors and time zones |
| Lowercase `snake_case` description | Consistent, shell-safe filenames |
| `.up.sql` suffix for forward migration | Explicit intent; tooling can enumerate up/down pairs |
| `.down.sql` suffix for rollback | Required unless migration is intentionally irreversible |
| Max 60 characters for the description part | Readable in terminal output and GitHub diffs |

### Good examples

```
migrations/20240115093000_create_ledger_headers.up.sql
migrations/20240115093000_create_ledger_headers.down.sql
migrations/20240201140500_add_fee_charged_column.up.sql
migrations/20240201140500_add_fee_charged_column.down.sql
migrations/20240310080000_add_index_on_transaction_hash.up.sql
migrations/20240310080000_add_index_on_transaction_hash.down.sql
```

### Bad examples

```
migrations/001_init.sql               ← No timestamp, ambiguous ordering
migrations/AddHorizonTable.sql        ← CamelCase, no timestamp, no up/down split
migrations/fix.sql                    ← Non-descriptive, no timestamp
migrations/20240115_create stuff.sql  ← Spaces in filename, incomplete timestamp
```

---

## File Structure

### Forward migration (`.up.sql`)

```sql
-- Migration: 20240115093000_create_ledger_headers
-- Description: Creates the ledger_headers table for Horizon ingestion.
-- Author: @your-github-handle
-- Date: 2024-01-15
-- Ticket: #1373

BEGIN;

-- 1. Create the table
CREATE TABLE IF NOT EXISTS ledger_headers (
    id              SERIAL          PRIMARY KEY,
    sequence        INTEGER         NOT NULL,
    hash            TEXT            NOT NULL UNIQUE,
    prev_hash       TEXT,
    bucket_list_hash TEXT,
    close_time      TIMESTAMPTZ     NOT NULL,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

-- 2. Add indexes
CREATE INDEX IF NOT EXISTS idx_ledger_headers_sequence
    ON ledger_headers (sequence);

-- 3. Record the migration
INSERT INTO schema_migrations (version, description, applied_at, checksum)
VALUES (
    '20240115093000',
    'create_ledger_headers',
    NOW(),
    1234567890  -- replace with actual CRC32 of this file
)
ON CONFLICT (version) DO NOTHING;

COMMIT;
```

### Rollback migration (`.down.sql`)

```sql
-- Rollback: 20240115093000_create_ledger_headers
-- WARNING: This will permanently delete all ledger header data.

BEGIN;

DROP INDEX IF EXISTS idx_ledger_headers_sequence;
DROP TABLE IF EXISTS ledger_headers;

DELETE FROM schema_migrations WHERE version = '20240115093000';

COMMIT;
```

---

## Forward Migration Requirements

Every `.up.sql` file must:

1. **Wrap all statements in an explicit transaction** (`BEGIN` / `COMMIT`).
   PostgreSQL DDL is transactional — a failure mid-migration must not leave a
   partially applied schema.

2. **Use `IF NOT EXISTS` / `IF EXISTS` guards** where applicable so the
   migration is safe to replay in development.

3. **Avoid `LOCK TABLE` with `ACCESS EXCLUSIVE`** unless absolutely necessary.
   Prefer `CREATE INDEX CONCURRENTLY` and `ALTER TABLE … ADD COLUMN` (which are
   non-blocking on Postgres 11+).

4. **Never truncate or delete existing rows** unless the change is intentional
   and explicitly approved in the PR description.

5. **Record itself in `schema_migrations`** as the final statement in the
   transaction.

6. **Include a header comment** with: version, description, author, date, and
   issue/ticket reference.

7. **Be idempotent** where technically feasible (use `IF NOT EXISTS`, `ON
   CONFLICT DO NOTHING`, etc.).

---

## Rollback Migration Requirements

Every `.down.sql` file must:

1. **Be the exact inverse** of the corresponding `.up.sql`. If the up migration
   creates a table, the down migration drops it. If the up migration adds a
   column, the down migration drops it.

2. **Wrap statements in an explicit transaction**.

3. **Use `IF EXISTS` guards** so the rollback is safe to replay if it was
   partially applied.

4. **Remove the `schema_migrations` record** as part of the rollback.

5. **Not destroy data that cannot be reconstructed** without explicit review
   approval. If the rollback is inherently destructive (e.g., dropping a
   populated table), annotate the file clearly:

   ```sql
   -- DESTRUCTIVE ROLLBACK: drops all data in ledger_headers.
   -- Approved by: @reviewer on 2024-01-20 (see PR #1374 comment)
   ```

### When a rollback is not required

Mark the migration as **intentionally irreversible** by creating an empty
`.down.sql` with a comment explaining why:

```sql
-- This migration is intentionally irreversible.
-- Reason: the data transformation cannot be undone without full re-ingestion
--         from the Stellar history archives.
-- Approved by: @lead-engineer
-- Alternatives considered: none viable within the operational window.
```

---

## Data Integrity Check Requirements

Every migration PR must include or update integrity checks in
`src/controller/migration_tests.rs`:

### Pre-migration checks (in the test harness setup)

- Assert that every table the migration reads from or modifies **already
  exists**.
- Assert expected row counts for tables that must not be empty before the
  migration runs.
- Assert that columns referenced in the migration's `WHERE` / `JOIN` clauses
  exist.

### Post-migration checks

- Assert that all **new tables and columns exist**.
- Assert that **dropped tables and columns are absent**.
- Assert that the **schema version incremented** by exactly 1.
- Assert that the **row count in unrelated tables is unchanged** (no accidental
  deletes).
- Assert that the **checksum of the applied migration** matches the on-disk file.

### Constraints to validate

| Check | Method |
|-------|--------|
| Table created | `assert_table_exists(table)` |
| Table dropped | `assert_table_absent(table)` |
| Column added | `assert_column_exists(table, column)` |
| Column dropped | `assert_column_absent(table, column)` |
| Row count unchanged | `assert_row_count(table, n)` |
| Schema version | `assert_schema_version(n)` |
| Checksum integrity | `assert_checksum_integrity(&migrations)` |

---

## Idempotency

Migrations should be idempotent so that:
- Development environments can be reset and re-migrated without manual
  intervention.
- Partial failures during CI can be retried automatically.

Idempotency techniques:

```sql
-- Tables
CREATE TABLE IF NOT EXISTS my_table ( ... );

-- Indexes
CREATE INDEX IF NOT EXISTS idx_name ON my_table (col);

-- Columns (Postgres 9.6+: no native IF NOT EXISTS, use DO block)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='my_table' AND column_name='my_col'
    ) THEN
        ALTER TABLE my_table ADD COLUMN my_col INT;
    END IF;
END $$;

-- Migration record
INSERT INTO schema_migrations (version, ...) VALUES (...)
ON CONFLICT (version) DO NOTHING;
```

---

## Examples

### Example 1: Adding a column to an existing table

**`migrations/20240201140500_add_fee_charged_column.up.sql`**

```sql
-- Migration: 20240201140500_add_fee_charged_column
-- Adds fee_charged column to transactions for Horizon API enrichment.
-- Author: @contributor
-- Ticket: #1200

BEGIN;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='transactions' AND column_name='fee_charged'
    ) THEN
        ALTER TABLE transactions ADD COLUMN fee_charged BIGINT DEFAULT 0 NOT NULL;
    END IF;
END $$;

INSERT INTO schema_migrations (version, description, applied_at)
VALUES ('20240201140500', 'add_fee_charged_column', NOW())
ON CONFLICT (version) DO NOTHING;

COMMIT;
```

**`migrations/20240201140500_add_fee_charged_column.down.sql`**

```sql
BEGIN;

ALTER TABLE transactions DROP COLUMN IF EXISTS fee_charged;
DELETE FROM schema_migrations WHERE version = '20240201140500';

COMMIT;
```

### Example 2: Creating a new table with a concurrent index

**`migrations/20240310080000_add_index_on_transaction_hash.up.sql`**

```sql
-- Migration: 20240310080000_add_index_on_transaction_hash
-- Adds a hash index to speed up transaction lookups.
-- Author: @contributor
-- Ticket: #1250

-- NOTE: CONCURRENTLY cannot run inside a transaction block.
--       This migration is run outside an explicit transaction.

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_transactions_hash
    ON transactions (hash);

INSERT INTO schema_migrations (version, description, applied_at)
VALUES ('20240310080000', 'add_index_on_transaction_hash', NOW())
ON CONFLICT (version) DO NOTHING;
```

> **Warning**: `CREATE INDEX CONCURRENTLY` cannot run inside a transaction.
> Mark such migrations with the comment above and ensure the migration runner
> is configured for `autocommit` mode for that file.

### Example 3: An irreversible migration

**`migrations/20240415120000_backfill_soroban_contract_types.up.sql`**

```sql
-- Migration: 20240415120000_backfill_soroban_contract_types
-- Backfills contract_type column from raw event data.
-- IRREVERSIBLE: backfilled data cannot be reconstructed without re-ingestion.
-- Author: @contributor
-- Approved by: @lead-engineer

BEGIN;

UPDATE soroban_events
SET contract_type = 'unknown'
WHERE contract_type IS NULL;

INSERT INTO schema_migrations (version, description, applied_at)
VALUES ('20240415120000', 'backfill_soroban_contract_types', NOW())
ON CONFLICT (version) DO NOTHING;

COMMIT;
```

**`migrations/20240415120000_backfill_soroban_contract_types.down.sql`**

```sql
-- This migration is intentionally irreversible.
-- The backfilled contract_type values cannot be derived without a full
-- re-ingestion from Stellar history archives.
-- Approved by: @lead-engineer on 2024-04-15
```

---

## Review Process & Checklist

### Author responsibilities

Before opening a PR containing a migration, the author must confirm all items
in this checklist:

- [ ] Migration file is named with a UTC timestamp (`YYYYMMDDHHMMSS`).
- [ ] Both `.up.sql` and `.down.sql` files are present (or rollback is
  documented as intentionally absent).
- [ ] Forward migration is wrapped in `BEGIN` / `COMMIT`.
- [ ] Rollback migration is wrapped in `BEGIN` / `COMMIT`.
- [ ] `CREATE TABLE IF NOT EXISTS` / `IF EXISTS` guards are used.
- [ ] `schema_migrations` is updated in the up and down files.
- [ ] A new test function in `migration_tests.rs` covers this migration's
  forward path.
- [ ] A new test function covers the rollback path (if a `.down.sql` exists).
- [ ] Pre-migration data integrity assertions are present.
- [ ] Post-migration data integrity assertions are present.
- [ ] The migration was tested locally with `scripts/test-migrations.sh`.
- [ ] No `ACCESS EXCLUSIVE` locks without justification.
- [ ] No `DROP TABLE` / `TRUNCATE` on tables containing data without explicit
  approval comment.

### Reviewer responsibilities

Reviewers must check:

1. **Semantic correctness**: Does the migration do what the PR description claims?
2. **Ordering safety**: Does the timestamp conflict with an existing migration?
3. **Rollback completeness**: Is the down migration the exact inverse of the up?
4. **Lock safety**: Does the migration avoid long-running locks?
5. **Test coverage**: Do the new test functions exercise all code paths?
6. **Integrity checks**: Are pre- and post-migration assertions sufficient?
7. **Idempotency**: Would running the migration twice cause an error?

### Approvals required

- At least **2 approvals** from engineers with database migration experience.
- For destructive migrations (DROP TABLE, TRUNCATE, DELETE): **1 additional
  approval** from the tech lead.

---

## CI Validation Requirements

The `.github/workflows/migration-tests.yml` workflow runs on every PR that
modifies files under `migrations/`, `src/controller/migration*.rs`, or
`scripts/test-migrations.sh`.

### Automated checks performed by CI

| Check | Tool | Must Pass |
|-------|------|-----------|
| Migration file naming convention | `scripts/test-migrations.sh lint` | Yes |
| Paired up/down files exist | `scripts/test-migrations.sh lint` | Yes |
| Forward migrations apply cleanly | `scripts/test-migrations.sh forward` | Yes |
| Rollback migrations apply cleanly | `scripts/test-migrations.sh rollback` | Yes |
| Data integrity after forward | Rust test harness | Yes |
| Data integrity after rollback | Rust test harness | Yes |
| No duplicate version timestamps | `scripts/test-migrations.sh lint` | Yes |
| Rust test suite passes | `cargo test` | Yes |

### Blocking the merge

Any failing check blocks the merge. There is no override — production database
safety is non-negotiable.

---

## Emergency Procedures

In the rare event that a migration must be applied to production outside the
normal process (e.g., a critical security patch):

1. Create a `hotfix/migration-YYYYMMDDHHMMSS` branch from `main`.
2. Apply the reduced checklist: naming, transaction wrapping, down migration,
   and at minimum a smoke test with `scripts/test-migrations.sh`.
3. Get emergency approval from the on-call tech lead.
4. Run `scripts/test-migrations.sh` against a production-replica database
   before applying to production.
5. Monitor node sync status for 15 minutes after applying.
6. Open a follow-up PR with full test coverage within 48 hours.

---

*Last updated: 2024-08-29 · Maintainer: Otowo Samuel*
