param(
  [int]$LocalPort = 3000,
  [string]$LogPath = ".\\localhostrun_live.log"
)

$ErrorActionPreference = "Continue"

while ($true) {
  try {
    $stamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    "[$stamp] Starting localhost.run tunnel for port $LocalPort" | Tee-Object -FilePath $LogPath -Append | Out-Null

    & ssh -n -N -T -o ExitOnForwardFailure=yes -o StrictHostKeyChecking=no -o ServerAliveInterval=15 -o ServerAliveCountMax=3 -R "80:localhost:$LocalPort" nokey@localhost.run 2>&1 |
      Tee-Object -FilePath $LogPath -Append | Out-Null

    $exitStamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    "[$exitStamp] Tunnel process ended. Reconnecting in 3 seconds..." | Tee-Object -FilePath $LogPath -Append | Out-Null
  } catch {
    $errStamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    "[$errStamp] Tunnel error: $($_.Exception.Message)" | Tee-Object -FilePath $LogPath -Append | Out-Null
  }

  Start-Sleep -Seconds 3
}
