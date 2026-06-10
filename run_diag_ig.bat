@echo off
cd /d C:\models\TradingBot
REM Front 7 IG creds live in the TradingBot secrets store (valid login),
REM NOT omni's default (_DEFAULT_SECRETS_PATH) which holds a defunct IG login.
REM Point this lane at the working creds. See 2026-06-10 Front 7 unblock.
set "KATE_SECRETS_PATH=C:\models\TradingBot\.mcp-brain\config\secrets.json"
"C:\Program Files\Python314\python.exe" -u -m trading_bot.supervisor.main --broker ig --symbols GBPUSD EURUSD AUDUSD EURGBP --strategy fx-london-breakout --fx-quantity 0.56 --trade-mode demo --db-path data\ig_front7_state.db --config-dir config --scid-dir . --log-level DEBUG --log-file logs\front7_ig_supervisor.log --no-trade-windows-utc "" > logs\ig-diag-stdout.log 2> logs\ig-diag-stderr.log
echo EXITCODE=%ERRORLEVEL%
