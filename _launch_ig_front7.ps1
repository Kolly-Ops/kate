$env:KATE_SECRETS_PATH = "C:\models\TradingBot\.mcp-brain\config\secrets.json"
Set-Location "C:\models\TradingBot"
$py = "C:\Program Files\Python314\python.exe"
$pyargs = @(
  "-u","-m","trading_bot.supervisor.main",
  "--broker","ig",
  "--symbols","GBPUSD","EURUSD","AUDUSD","EURGBP",
  "--strategy","fx-london-breakout",
  "--fx-quantity","0.56",
  "--trade-mode","demo",
  "--db-path","data\ig_front7_state.db",
  "--config-dir","config",
  "--scid-dir",".",
  "--log-level","INFO",
  "--log-file","logs\front7_ig_supervisor.log"
)
$p = Start-Process -FilePath $py -ArgumentList $pyargs -WorkingDirectory "C:\models\TradingBot" -WindowStyle Hidden -PassThru
Write-Output ("Launched IG Front 7 supervisor PID=" + $p.Id)
