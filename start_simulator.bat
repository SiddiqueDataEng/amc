@echo off
REM === Start AMC Simulator (Flask UI) ===

cd /d %~dp0\simulator
call ..\venv\Scripts\activate.bat

echo Starting Flask Simulator...
python app.py

pause
