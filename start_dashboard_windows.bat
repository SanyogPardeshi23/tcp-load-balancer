@echo off
REM One-click demo: starts 3 backends + the load balancer + the dashboard and opens the browser.
REM Stop everything with Ctrl+C in this window (not by closing it).
cd /d "%~dp0"
python dashboard.py
pause
