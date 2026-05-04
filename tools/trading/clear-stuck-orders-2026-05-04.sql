-- Kate stuck-order cleanup — 2026-05-04 (drift cycle 2)
--
-- Purpose: Remove the ~20 stuck PENDING orders from the 2026-05-04
-- session that accumulated while Sierra Chart's trading session was
-- uninitialised after the 22:00 UTC Globex reopen.
--
-- Diagnosed by COO Gemini 2026-05-04 morning via direct VPS query.
-- Root cause: Hypothesis (α) — Sierra DTC up but trading session not
-- initialised; TradeActivityLog file missing for today. Submits sent,
-- never acknowledged with broker order_id.
--
-- Companion script to tools/trading/clear-stale-pending-orders.sql.
-- Same safety pattern: backup-then-delete in single transaction.
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
UNION ALL SELECT 'stuck PENDING from today (>=2026-05-04, order_id IS NULL)',
  COUNT(*) FROM orders
  WHERE status = 'PENDING'
    AND submitted_at >= '2026-05-04'
    AND fill_price IS NULL;  -- order_id field not in schema; using fill_price IS NULL as proxy for "no broker ack"

.print
.print '=== KILL_SWITCH STATE ==='
SELECT * FROM kill_switch;

-- ───────────────────────────────────────────────────────────────────
-- Mutation block — single transaction, backup-then-delete
-- ───────────────────────────────────────────────────────────────────
BEGIN TRANSACTION;

-- Backup table (created if absent; insert is idempotent on re-runs)
CREATE TABLE IF NOT EXISTS orders_backup_2026_05_04 AS
  SELECT * FROM orders WHERE 0;

INSERT INTO orders_backup_2026_05_04
  SELECT * FROM orders
  WHERE status = 'PENDING'
    AND submitted_at >= '2026-05-04'
    AND client_order_id NOT IN (
      SELECT client_order_id FROM orders_backup_2026_05_04
    );

-- Surgical delete: only PENDING orders submitted on or after 2026-05-04
-- with no fill data (i.e. broker never acknowledged)
DELETE FROM orders
WHERE status = 'PENDING'
  AND submitted_at >= '2026-05-04'
  AND fill_price IS NULL;

COMMIT;

-- ───────────────────────────────────────────────────────────────────
-- Post-cleanup verification (read-only)
-- ───────────────────────────────────────────────────────────────────
.print
.print '=== POST-CLEANUP STATE ==='
SELECT 'total orders' AS metric, COUNT(*) AS value FROM orders
UNION ALL SELECT 'remaining stuck PENDING (>=2026-05-04)',
  COUNT(*) FROM orders WHERE status = 'PENDING' AND submitted_at >= '2026-05-04'
UNION ALL SELECT 'rows backed up to orders_backup_2026_05_04',
  COUNT(*) FROM orders_backup_2026_05_04;

.print
.print '=== EXPECTED OUTCOMES ==='
.print '  remaining stuck PENDING (>=2026-05-04) = 0'
.print '  rows backed up                        = ~20 (depends on count at run time)'
.print
.print 'NOTE: kill_switch state NOT touched by this script. The recovery'
.print '      plan separately handles trip-and-reset around Sierra GUI restart.'
.print
