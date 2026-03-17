@echo off
cd /d "e:\projects\GG hackthon\compact-bi-demo"
powershell -NoProfile -ExecutionPolicy Bypass -File "e:\projects\GG hackthon\compact-bi-demo\keep-tunnel.ps1" -LocalPort 3000 -LogPath "e:\projects\GG hackthon\compact-bi-demo\localhostrun_live.log"
