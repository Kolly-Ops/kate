-- Kate stale-bracket cleanup — 2026-05-03
--
-- Purpose: Remove the 82 stale PENDING orders from the 2026-04-30 paper
-- trading session that are accumulating drift while the kill-switch
-- suppresses lifecycle events. Reset kill-switch once cleared so paper
-- validation can resume.
--
-- Diagnosed by COO Gemini 2026-05-03 19:00 UTC via direct VPS query.
-- Hypothesis (b) from Co-CEO Claude's diagnostic tree (resurgence handoff).
--
-- IMPORTANT — schema notes:
--   - `kill_switch` columns are `state` / `reason` / `since` / `updated_at`,
--     NOT `status` / `triggered_at`. Original cleanup-instructions handoff
--     used the wrong column names; this script uses the schema-correct
--     versions per `trading_bot/core/state/state_store.py:123-129`.
--   - `since` is NOT NULL in the schema, so it gets set to the cleanup
--     timestamp rather than NULL.
--
-- Safety: wrapped in a single transaction with a backup table written
-- BEFORE the DELETE so the rows can be recovered if anything looks wrong
-- post-COMMIT. Run this via sqlite3 with `.read` for transactional
-- semantics. Pre/post counts printed to stdout for verification.
--
-- Approval class: APPROVAL — destructive production state mutation.
-- Do not run without explicit CEO sign-off.

.headers on
.mode column

-- ───────────────────────────────────────────────────────────────────
-- Pre-cleanup verification (read-only)
-- ───────────────────────────────────────────────────────────────────
.print
.print '=== PRE-CLEANUP STATE ==='
SELECT 'total orders' AS metric, COUNT(*) AS value FROM orders
UNION ALL SELECT 'orders by status', NULL
UNION ALL
  SELECT '  ' || status, COUNT(*) FROM orders GROUP BY status
UNION ALL SELECT 'stale PENDING (pre-2026-05-01)',
  COUNT(*) FROM orders WHERE status = 'PENDING' AND submitted_at < '2026-05-01';

.print
.print '=== KILL_SWITCH STATE ==='
SELECT * FROM kill_switch;

-- ───────────────────────────────────────────────────────────────────
-- Mutation block — single transaction, backup-then-delete
-- ───────────────────────────────────────────────────────────────────
BEGIN TRANSACTION;

-- Backup table (created if absent; insert is idempotent on re-runs)
CREATE TABLE IF NOT EXISTS orders_backup_2026_05_03 AS
  SELECT * FROM orders WHERE 0;

-- Snapshot the rows we're about to delete
INSERT INTO orders_backup_2026_05_03
  SELECT * FROM orders
  WHERE status = 'PENDING'
    AND submitted_at < '2026-05-01'
    AND client_order_id NOT IN (
      SELECT client_order_id FROM orders_backup_2026_05_03
    );

-- Surgical delete: only PENDING orders submitted before 2026-05-01
DELETE FROM orders
WHERE status = 'PENDING'
  AND submitted_at < '2026-05-01';

-- Reset kill-switch with correct schema columns
UPDATE kill_switch
SET state      = 'INACTIVE',
    reason     = 'cleanup-2026-05-03: 82 stale brackets from 2026-04-30 cleared; drift root cause (hypothesis b) confirmed by COO Gemini',
    since      = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
WHERE id = 1;

COMMIT;

-- ───────────────────────────────────────────────────────────────────
-- Post-cleanup verification (read-only)
-- ───────────────────────────────────────────────────────────────────
.print
.print '=== POST-CLEANUP STATE ==='
SELECT 'total orders' AS metric, COUNT(*) AS value FROM orders
UNION ALL SELECT 'remaining stale PENDING (pre-2026-05-01)',
  COUNT(*) FROM orders WHERE status = 'PENDING' AND submitted_at < '2026-05-01'
UNION ALL SELECT 'rows backed up',
  COUNT(*) FROM orders_backup_2026_05_03;

.print
.print '=== KILL_SWITCH POST-RESET ==='
SELECT * FROM kill_switch;

.print
.print '=== EXPECTED OUTCOMES ==='
.print '  remaining stale PENDING (pre-2026-05-01) = 0'
.print '  rows backed up                           = 82  (or close)'
.print '  kill_switch.state                        = INACTIVE'
.print
