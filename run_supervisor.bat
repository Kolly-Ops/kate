@echo off
cd C:\models\TradingBot
start "KateSupervisor" /B "C:\Program Files\Python314\python.exe" -u -m trading_bot.supervisor.main --broker mt5 --symbols GBPUSD EURUSD AUDUSD EURGBP --strategy fx-london-breakout --fx-quantity 0.56 --db-path data\mt5_front4_state.db --config-dir config --scid-dir . --log-level DEBUG --log-file logs\mt5-supervisor.log --no-trade-windows-utc ""
