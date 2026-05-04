@echo off
REM =====================================================================
REM Kate drift diagnostic — 2026-05-04
REM
REM Runs all four diagnostic queries in sequence and writes results to
REM C:\kate\drift-diagnosis-2026-05-04.txt for SCP back to the operator.
REM
REM No mutation, all read-only. Safe to run while Kate is running.
REM =====================================================================

set OUT=C:\kate\drift-diagnosis-2026-05-04.txt
set SC_LOGS=C:\SierraChart\TradeActivityLogs
set KATE_LOG=C:\kate\kate-paper.log
set KATE_BAT=C:\kate\run-kate-paper.bat

echo Kate drift diagnostic — %DATE% %TIME% > "%OUT%"
echo ===================================== >> "%OUT%"
echo. >> "%OUT%"

echo. >> "%OUT%"
echo ##### QUERY 1 — TradeActivityLog filenames for today (filename = data) ##### >> "%OUT%"
echo. >> "%OUT%"
dir "%SC_LOGS%\TradeActivityLog_2026-05-04*" >> "%OUT%" 2>&1
echo. >> "%OUT%"
echo Interpretation: >> "%OUT%"
echo   .Sim1.simulated.data = routing context exists ^(rules out alpha^) >> "%OUT%"
echo   .None.simulated.data = empty TradeAccount routing ^(alpha confirmed^) >> "%OUT%"
echo   Multiple files       = routing flipped mid-day; mtimes show when >> "%OUT%"
echo. >> "%OUT%"

echo. >> "%OUT%"
echo ##### QUERY 2 — TradeActivityLog content for today's client_order_ids ##### >> "%OUT%"
echo. >> "%OUT%"
findstr /C:"atrbo-MESM26-26050" "%SC_LOGS%\TradeActivityLog_2026-05-04*.data" >> "%OUT%" 2>&1
echo. >> "%OUT%"
echo Interpretation: >> "%OUT%"
echo   "Trade Account is empty"  -- alpha confirmed ^(same as 2026-04-29^) >> "%OUT%"
echo   "symbol not found"         -- gamma confirmed ^(symbol roll^) >> "%OUT%"
echo   "account ... not found"    -- delta confirmed ^(account renamed^) >> "%OUT%"
echo   "order filled @"           -- orders ARE filling, ORDER_UPDATE handler bug >> "%OUT%"
echo   nothing                    -- submits not reaching Sierra ^(epsilon or DTC bug^) >> "%OUT%"
echo. >> "%OUT%"

echo. >> "%OUT%"
echo ##### QUERY 3a — current run-kate-paper.bat contents ##### >> "%OUT%"
echo. >> "%OUT%"
type "%KATE_BAT%" >> "%OUT%" 2>&1
echo. >> "%OUT%"

echo. >> "%OUT%"
echo ##### QUERY 3b — running python processes with command lines ##### >> "%OUT%"
echo. >> "%OUT%"
powershell -NoProfile -Command "Get-CimInstance Win32_Process -Filter \"Name='python.exe'\" | Select-Object ProcessId, CommandLine | Format-List" >> "%OUT%" 2>&1
echo. >> "%OUT%"
echo Interpretation: >> "%OUT%"
echo   Confirm command line includes --trade-account Sim1 --submit-trade-account Sim1 >> "%OUT%"
echo   If different, watchdog regression ^(epsilon^) >> "%OUT%"
echo. >> "%OUT%"

echo. >> "%OUT%"
echo ##### QUERY 4 — kate-paper.log around 00:50-01:10 UTC today ##### >> "%OUT%"
echo. >> "%OUT%"
powershell -NoProfile -Command "Get-Content '%KATE_LOG%' | Select-String -Pattern '2026-05-04T00:5[0-9]|2026-05-04T01:0[0-9]'" >> "%OUT%" 2>&1
echo. >> "%OUT%"
echo Interpretation: >> "%OUT%"
echo   Look for outbound msg_type=208 ^(submit^) lines — confirms engine submitting >> "%OUT%"
echo   Look for inbound msg_type=301 ^(order update^) — should follow each submit >> "%OUT%"
echo   Count of 208 vs 301 within the same 5-min window tells the silent-drop story >> "%OUT%"
echo. >> "%OUT%"

echo. >> "%OUT%"
echo ##### BONUS — Sierra TradeService Window: any recent restart entries? ##### >> "%OUT%"
echo. >> "%OUT%"
powershell -NoProfile -Command "Get-WinEvent -LogName System -MaxEvents 200 | Where-Object {$_.TimeCreated -gt (Get-Date).AddHours(-12) -and ($_.Message -match 'Sierra' -or $_.Message -match 'TradeService')} | Format-Table TimeCreated, Id, LevelDisplayName, ProviderName -AutoSize" >> "%OUT%" 2>&1
echo. >> "%OUT%"
echo Interpretation: >> "%OUT%"
echo   Any entries in last 12h hinting Sierra TradeService restarted = beta plausible >> "%OUT%"
echo. >> "%OUT%"

echo. >> "%OUT%"
echo ===================================== >> "%OUT%"
echo Diagnostic complete. Output saved to %OUT% >> "%OUT%"
echo Operator: SCP this file back to your workstation. >> "%OUT%"

echo Done. Output saved to %OUT%
echo SCP it back with:
echo   scp Administrator@149.102.150.132:%OUT% .
