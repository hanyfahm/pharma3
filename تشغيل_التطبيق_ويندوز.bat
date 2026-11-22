@echo off
chcp 65001 >nul
echo ==========================================
echo   Saydaliyati - Pharmacy Warehouse System
echo ==========================================
where node >nul 2>nul
if %errorlevel% neq 0 (
  echo [ERROR] Please install Node.js v20+ from https://nodejs.org
  pause
  exit /b 1
)
if not exist node_modules (
  echo Installing dependencies for first run...
  call npm install
)
start "" http://localhost:3000
call npm run dev
pause
