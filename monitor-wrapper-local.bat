@echo off
REM monitor-wrapper-local.bat — Wiltshire watchdog for Kate ORB engine
REM
REM Auto-restarts Kate if the supervisor process exits for any reason
REM (crash, network blip, Sierra restart, OOM, etc). Mirrors the VPS
REM monitor-wrapper.bat pattern. Run this once detached and Kate will
REM keep itself alive without further intervention.
REM
REM Usage:
REM   Foreground (this window stays open, easy to read):
REM     monitor-wrapper-local.bat
REM   Detached (closes-safe; minimised in taskbar):
REM     start "" /min cmd /c monitor-wrapper-local.bat
REM
REM Stop method: kill the python.exe process via Task Manager OR close
REM this watchdog window AND kill the Python child process.

cd /d %~dp0

if not exist logs mkdir logs

set RESTART_COUNT=0

:LOOP
set /a RESTART_COUNT=RESTART_COUNT+1
echo [%date% %time%] watchdog: launching Kate (restart #%RESTART_COUNT%) >> logs\watchdog.log
echo [%date% %time%] watchdog: launching Kate (restart #%RESTART_COUNT%)

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
  --log-file "%~dp0logs\local-lab.log" ^
  --require-trade-activity-suffix "None"

REM ── Kate exited; back-off then relaunch ─────────────────────────────
REM A 30-second wait avoids hot-restart loops if Kate dies on startup
REM (e.g. Sierra Chart not running). 30s is short enough that a
REM transient blip recovers fast, long enough that DNS/Sierra/etc.
REM have time to settle.
echo [%date% %time%] watchdog: Kate exited (code %errorlevel%); restarting in 30s >> logs\watchdog.log
echo [%date% %time%] watchdog: Kate exited (code %errorlevel%); restarting in 30s
timeout /t 30 /nobreak >nul

goto LOOP
