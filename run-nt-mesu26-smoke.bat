@echo off
cd /d C:\models\TradingBot
REM Idempotency guard (keepalive-safe): skip if a --broker ninja supervisor already runs.
powershell -NoProfile -Command "if (Get-CimInstance Win32_Process | Where-Object { $_.Name -eq 'python.exe' -and $_.CommandLine -like '*broker ninja*' }) { exit 0 } else { exit 1 }"
if %errorlevel% equ 0 exit /b 0
"C:\Program Files\Python314\python.exe" -u -m trading_bot.supervisor.main --broker ninja --strategy orb --symbols MESU26 --orb-reward-risk 2.5 --orb-direction both --orb-ema-period 200 --atr-stop-mult 1.1 --db-path data\nt_front5_state.db --log-file logs\nt-supervisor.log --log-level DEBUG --trade-mode demo --no-trade-windows-utc ""
