@echo off
cd C:\models\TradingBot
start "KateSupervisorIG" /B "C:\Program Files\Python314\python.exe" -u -m trading_bot.supervisor.main --broker ig --symbols GBPUSD EURUSD AUDUSD EURGBP --strategy fx-london-breakout --fx-quantity 0.56 --trade-mode demo --db-path data\ig_front7_state.db --config-dir config --scid-dir . --log-level INFO --log-file logs\front7_ig_supervisor.log --no-trade-windows-utc ""
