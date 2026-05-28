@echo off
setlocal

cd /d "%~dp0\.."

if exist ".venv\Scripts\python.exe" (
    ".venv\Scripts\python.exe" -c "import encodings" >nul 2>nul
    if not errorlevel 1 (
        ".venv\Scripts\python.exe" "Tools\db_admin_tkinter.py"
        exit /b %errorlevel%
    )
)

py -3 -c "import encodings" >nul 2>nul
if not errorlevel 1 (
    py -3 "Tools\db_admin_tkinter.py"
    exit /b %errorlevel%
)

python -c "import encodings" >nul 2>nul
if not errorlevel 1 (
    python "Tools\db_admin_tkinter.py"
    exit /b %errorlevel%
)

echo Python was not found, or the project virtualenv is broken.
echo Install Python or recreate .venv, then run Tools\db_admin_tkinter.py.
pause
exit /b 1
