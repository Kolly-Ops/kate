@echo off
cd /d "%~dp0"
start "KateSupervisorMT5-NY" /B "C:\Program Files\Python314\python.exe" -u -m trading_bot.supervisor.main --broker mt5 --symbols GBPUSD EURUSD AUDUSD USDCAD --strategy fx-ny-breakout --fx-quantity 0.56 --db-path data\mt5_front4_ny_state.db --config-dir config --scid-dir . --log-level DEBUG --log-file logs\mt5-supervisor-ny.log --no-trade-windows-utc ""
