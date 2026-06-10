@echo off
cd C:\models\TradingBot
REM Front 7 IG creds live in the TradingBot secrets store (valid login),
REM NOT omni's default which holds a defunct IG login. See 2026-06-10 unblock.
set "KATE_SECRETS_PATH=C:\models\TradingBot\.mcp-brain\config\secrets.json"
start "KateSupervisorIG" /B "C:\Program Files\Python314\python.exe" -u -m trading_bot.supervisor.main --broker ig --symbols GBPUSD EURUSD AUDUSD EURGBP --strategy fx-london-breakout --fx-quantity 0.56 --trade-mode demo --db-path data\ig_front7_state.db --config-dir config --scid-dir . --log-level INFO --log-file logs\front7_ig_supervisor.log --no-trade-windows-utc ""
