@echo off
REM run-kate-local.bat — Wiltshire Lab Rig (Bypass Mode)
REM Points to local Sierra Chart on port 11099 in Sim-Only mode.
REM Overrides suffix check to accept .None.data for local SC Data feed lab.

cd /d %~dp0

REM Idempotency check
powershell -NoProfile -Command "if (Get-CimInstance Win32_Process -Filter 'Name=''python.exe''' | Where-Object { $_.CommandLine -like '*trading_bot.supervisor*' }) { echo 'Kate already running locally.'; exit 0 } else { exit 1 }"
if %errorlevel% equ 0 exit /b 0

echo [%date% %time%] lab: launching Kate (VPS Track 1 - Sim-Only) >> logs\local-lab.log

python -u -m trading_bot.supervisor.main ^
  --symbols MESU26 ^
  --scid-dir "C:\SierraChart\Data" ^
  --dtc-host 127.0.0.1 ^
  --dtc-port 11099 ^
  --trade-mode demo ^
  --trade-account Sim1 ^
  --submit-trade-account Sim1 ^
  --trade-activity-logs-dir "C:\SierraChart\TradeActivityLogs" ^
  --log-level INFO ^
  --log-file logs\local-lab.log ^
  --require-trade-activity-suffix "None" ^
  --allow-no-trade-activity-log
