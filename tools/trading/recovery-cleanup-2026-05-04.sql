-- Kate recovery cleanup — 2026-05-04 cycle 2
-- Combined Steps 5 + 6: surgical DELETE + kill_switch reset, in one
-- transaction, in one .read invocation. Avoids all PowerShell/SSH/
-- sqlite3 quoting friction.
--
-- Usage on Kate VPS:
--   sqlite3 C:\kate\state.db ".read C:\kate\recovery-cleanup-2026-05-04.sql"
--
-- Backup table 'orders_backup_2026_05_04' written before DELETE for
-- reversibility. Different table from yesterday's
-- orders_backup_2026_05_03 to avoid conflation.
--
-- Approval class: APPROVAL — destructive production state mutation.
-- CEO has approved via 2026-05-04 morning recovery sequence.

.headers on
.mode column

-- ───────────────────────────────────────────────────────────────────
-- Pre-cleanup verification (read-only)
-- ───────────────────────────────────────────────────────────────────
.print
.print '=== PRE-RECOVERY STATE ==='
SELECT 'total orders' AS metric, COUNT(*) AS value FROM orders
UNION ALL SELECT 'orders by status', NULL
UNION ALL
  SELECT '  ' || status, COUNT(*) FROM orders GROUP BY status
UNION ALL SELECT 'stuck PENDING from today (>=2026-05-04, no fill)',
  COUNT(*) FROM orders
  WHERE status = 'PENDING'
    AND submitted_at >= '2026-05-04'
    AND fill_price IS NULL;

.print
.print '=== KILL_SWITCH PRE-RESET ==='
SELECT * FROM kill_switch;

-- ───────────────────────────────────────────────────────────────────
-- Mutation block — single transaction
-- ───────────────────────────────────────────────────────────────────
BEGIN TRANSACTION;

-- 5a — Backup table
CREATE TABLE IF NOT EXISTS orders_backup_2026_05_04 AS
  SELECT * FROM orders WHERE 0;

-- 5b — Snapshot rows about to be deleted (idempotent on re-run)
INSERT INTO orders_backup_2026_05_04
  SELECT * FROM orders
  WHERE status = 'PENDING'
    AND submitted_at >= '2026-05-04'
    AND fill_price IS NULL
    AND client_order_id NOT IN (
      SELECT client_order_id FROM orders_backup_2026_05_04
    );

-- 5c — Surgical delete: stuck-today PENDING with no broker ack
DELETE FROM orders
WHERE status = 'PENDING'
  AND submitted_at >= '2026-05-04'
  AND fill_price IS NULL;

-- 6 — Reset kill_switch with the load-bearing caveat
-- (Note: codebase grep confirms neither supervisor nor engine reads
-- kill_switch directly. This UPDATE is dashboard banner + risk-manager
-- trip-condition lookup, NOT engine gating. Kate restart is NOT
-- required to pick up this change.)
UPDATE kill_switch
SET state      = 'INACTIVE',
    reason     = 'cleanup-2026-05-04 paper-validation only. NOT live-flip clearance. Gates in protocol/kate-pre-live-flip-gate.md must clear + CEO+COO sign-off before live cash. Recovery from drift cycle 2 (Sierra session-init failure at 22:00 UTC Globex reopen).',
    since      = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
WHERE id = 1;

COMMIT;

-- ───────────────────────────────────────────────────────────────────
-- Post-cleanup verification (read-only)
-- ───────────────────────────────────────────────────────────────────
.print
.print '=== POST-RECOVERY STATE ==='
SELECT 'total orders' AS metric, COUNT(*) AS value FROM orders
UNION ALL SELECT 'remaining stuck PENDING (>=2026-05-04)',
  COUNT(*) FROM orders WHERE status = 'PENDING' AND submitted_at >= '2026-05-04'
UNION ALL SELECT 'rows backed up (orders_backup_2026_05_04)',
  COUNT(*) FROM orders_backup_2026_05_04;

.print
.print '=== KILL_SWITCH POST-RESET ==='
SELECT * FROM kill_switch;

.print
.print '=== EXPECTED OUTCOMES ==='
.print '  remaining stuck PENDING (>=2026-05-04)    = 0'
.print '  rows backed up                            = ~20'
.print '  kill_switch.state                         = INACTIVE'
.print '  kill_switch.reason starts with            = cleanup-2026-05-04 paper-validation only. NOT live-flip...'
.print
