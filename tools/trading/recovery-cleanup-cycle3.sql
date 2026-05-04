-- Kate cycle 3 recovery cleanup — 2026-05-04 evening
--
-- Same backup-then-delete-then-reset pattern as morning cycle 2.
-- Different backup table (cycle3) to keep audit trail distinct.
--
-- Targets the 3 known stuck PENDING from late afternoon:
--   atrbo-MESM26-260504160100  (submitted ~16:01 UTC)
--   atrbo-MESM26-260504161800  (submitted ~16:18 UTC)
--   atrbo-MESM26-260504161900  (submitted ~16:19 UTC)
--   atrbo-MESM26-260504170100  (submitted ~17:01 UTC)
-- Filter is 'submitted_at >= 2026-05-04 16:00' to catch any others in
-- that bucket without enumerating client_order_ids.
--
-- Approval class: APPROVAL — destructive production state mutation.
-- CEO approved via "i want this sorted asap" 2026-05-04 evening.

.headers on
.mode column

.print
.print '=== PRE-CYCLE-3-CLEANUP STATE ==='
SELECT 'orders by status' AS metric, NULL AS value
UNION ALL SELECT '  ' || status, COUNT(*) FROM orders GROUP BY status
UNION ALL SELECT 'stuck PENDING from afternoon (>= 16:00 UTC, no fill)',
  COUNT(*) FROM orders
  WHERE status = 'PENDING'
    AND submitted_at >= '2026-05-04T16:00'
    AND fill_price IS NULL;

.print
.print '=== KILL_SWITCH PRE-RESET ==='
SELECT * FROM kill_switch;

BEGIN TRANSACTION;

CREATE TABLE IF NOT EXISTS orders_backup_cycle3 AS
  SELECT * FROM orders WHERE 0;

INSERT INTO orders_backup_cycle3
  SELECT * FROM orders
  WHERE status = 'PENDING'
    AND submitted_at >= '2026-05-04T16:00'
    AND fill_price IS NULL
    AND client_order_id NOT IN (
      SELECT client_order_id FROM orders_backup_cycle3
    );

DELETE FROM orders
WHERE status = 'PENDING'
  AND submitted_at >= '2026-05-04T16:00'
  AND fill_price IS NULL;

UPDATE kill_switch
SET state      = 'INACTIVE',
    reason     = 'cleanup-cycle3-2026-05-04 paper-validation only. NOT live-flip clearance. Gates in protocol/kate-pre-live-flip-gate.md must clear + CEO+COO sign-off before live cash. Recovery from cycle 3 (Sierra reverted to E8933 live mode mid-session — third Sierra mode regression in 24h; structural fix pending).',
    since      = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
WHERE id = 1;

COMMIT;

.print
.print '=== POST-CYCLE-3-CLEANUP STATE ==='
SELECT 'remaining stuck PENDING (>= 16:00 UTC)' AS metric,
  COUNT(*) AS value FROM orders WHERE status = 'PENDING' AND submitted_at >= '2026-05-04T16:00'
UNION ALL SELECT 'rows backed up (orders_backup_cycle3)',
  COUNT(*) FROM orders_backup_cycle3;

.print
.print '=== KILL_SWITCH POST-RESET ==='
SELECT * FROM kill_switch;
.print
