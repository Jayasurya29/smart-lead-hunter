@echo off
title Smart Lead Hunter - Startup
echo ============================================
echo   SMART LEAD HUNTER - Starting All Services
echo ============================================
echo.

:: 1. Start Docker Desktop if not running
tasklist /FI "IMAGENAME eq Docker Desktop.exe" 2>NUL | find /I "Docker Desktop.exe" >NUL
if %ERRORLEVEL% NEQ 0 (
    echo [1/5] Starting Docker Desktop...
    start "" "C:\Program Files\Docker\Docker\Docker Desktop.exe"
    echo       Waiting for Docker to initialize...
    timeout /t 30 /nobreak >NUL
) else (
    echo [1/5] Docker Desktop already running
)

:: 2. Wait for the Docker engine to actually answer
echo [2/5] Waiting for Docker engine...
:wait_docker
docker ps >NUL 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo       ...Docker not ready yet, waiting 5s
    timeout /t 5 /nobreak >NUL
    goto wait_docker
)

:: Start the containers (REAL names) and give them a moment to come up
echo       Starting containers...
docker start smart-lead-hunter-redis smart-lead-hunter-db >NUL 2>&1
echo       Waiting 12s for Redis + Postgres to be ready...
timeout /t 12 /nobreak >NUL
echo       Containers started.

:: 3. Start Celery Worker
echo [3/5] Starting Celery Worker...
start "SLH-Worker" cmd /k "cd /d C:\Users\it2\smart-lead-hunter && venv\Scripts\activate && celery -A app.tasks.celery_app worker --loglevel=info --pool=solo"
timeout /t 3 /nobreak >NUL

:: 4. Start Celery Beat
echo [4/5] Starting Celery Beat...
start "SLH-Beat" cmd /k "cd /d C:\Users\it2\smart-lead-hunter && venv\Scripts\activate && celery -A app.tasks.celery_app beat --loglevel=info"
timeout /t 3 /nobreak >NUL

:: 5. Start Uvicorn
echo [5/5] Starting Uvicorn...
start "SLH-Server" cmd /k "cd /d C:\Users\it2\smart-lead-hunter && venv\Scripts\activate && python -m uvicorn app.main:app --host 0.0.0.0 --port 8000"

echo.
echo ============================================
echo   ALL SERVICES STARTED!
echo   Dashboard: http://192.168.1.152:8000
echo ============================================
echo.
echo You can close this window.
pause
