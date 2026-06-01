@echo off
"python" -u -m trading_bot.supervisor.main --broker ig --symbols GBPUSD EURUSD AUDUSD EURGBP --strategy fx-london-breakout --fx-quantity 0.56 --trade-mode demo --db-path data\ig_front7_state.db --config-dir config --scid-dir . --log-level INFO --log-file logs\front7_ig_supervisor.log --no-trade-windows-utc "" > logs\ig_diag.log 2>&1
