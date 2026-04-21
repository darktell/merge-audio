@echo off
REM Launcher for merge_audio.py on Windows.
REM Double-click to run. A folder picker will appear.
REM Default: 4 parallel jobs. Change -j below if needed:
REM   -j 1 = serial (safest on HDD)
REM   -j 4 = 4 parallel (good for SSD)

cd /d "%~dp0"
python merge_audio.py -j 2
echo.
pause