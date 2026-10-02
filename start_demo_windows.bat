@echo off
REM Classic demo: every component in its own titled window.
REM Close the "web-2 :9002" window (or Ctrl+C in it) to simulate a server crash.
cd /d "%~dp0"

start "web-1 :9001" cmd /k python backend_server.py --port 9001 --name web-1
start "web-2 :9002" cmd /k python backend_server.py --port 9002 --name web-2
start "web-3 :9003 (slow)" cmd /k python backend_server.py --port 9003 --name web-3 --delay 150
timeout /t 1 >nul
start "LOAD BALANCER :8080" cmd /k python load_balancer.py --algo rr --interval 1 --status-every 10
timeout /t 2 >nul
start "CLIENT" cmd /k python client_loadgen.py --watch --gap 0.3

echo.
echo Stats page: http://127.0.0.1:8081/
echo To simulate a crash: close the "web-2 :9002" window (or press Ctrl+C in it).
