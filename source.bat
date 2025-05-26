@echo off
setlocal

rem === Virtual Environment Activation Script for Windows ===

if not exist ".venv\Scripts\activate.bat" (
    echo [ERROR] .venv not found. Run "python -m venv .venv" first.
    exit /b 1
)

echo [INFO] Activating virtual environment...
call .venv\Scripts\activate.bat

rem === Install dependencies automatically ===
if exist requirements.txt (
    echo [INFO] Installing runtime requirements...
    pip install -r requirements.txt
)

if exist requirements-dev.txt (
    echo [INFO] Installing development requirements...
    pip install -r requirements-dev.txt
)

echo [SUCCESS] Environment is ready!
endlocal
