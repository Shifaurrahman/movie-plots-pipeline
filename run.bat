@echo off
REM Windows batch script to run the ETL pipeline

if "%1"=="all" goto all
if "%1"=="pipeline" goto pipeline
if "%1"=="query" goto query
if "%1"=="clean" goto clean
if "%1"=="setup" goto setup
if "%1"=="" goto all
goto help

:all
echo Running complete pipeline...
call :setup
call :pipeline
call :query
goto end

:setup
echo.
echo 📦 Installing dependencies...
pip install -r requirements.txt
goto end

:pipeline
echo.
echo 🚀 Running ETL pipeline...
python src/pipeline.py
goto end

:query
echo.
echo 🔍 Running queries...
python src/query.py
goto end

:clean
echo.
echo 🧹 Cleaning output directory...
if exist output rmdir /s /q output
echo ✅ Clean complete
goto end

:help
echo Available commands:
echo   run.bat all       - Run complete pipeline
echo   run.bat setup     - Install dependencies
echo   run.bat pipeline  - Run ETL pipeline only
echo   run.bat query     - Run query script only
echo   run.bat clean     - Remove output directory
goto end

:end