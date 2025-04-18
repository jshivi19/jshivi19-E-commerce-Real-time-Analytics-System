@echo off
setlocal enabledelayedexpansion

echo Setting up E-commerce Analytics testing environment...

REM Check Python version
python --version > nul 2>&1
if errorlevel 1 (
    echo Python is not installed!
    exit /b 1
)

for /f "tokens=2" %%I in ('python --version 2^>^&1') do set PYTHON_VERSION=%%I
echo Found Python version: %PYTHON_VERSION%

REM Create and activate virtual environment
echo Creating virtual environment...
if not exist venv (
    python -m venv venv
)

REM Activate virtual environment
call venv\Scripts\activate.bat

REM Install dependencies
echo Installing dependencies...
python -m pip install --upgrade pip
pip install -r requirements.txt

REM Run style checks
echo Running style checks...
echo Running black...
black --check src tests
if errorlevel 1 (
    echo Style check failed! Please run 'black src tests' to format the code.
    exit /b 1
)

echo Running flake8...
flake8 src tests
if errorlevel 1 (
    echo Linting failed! Please fix the issues above.
    exit /b 1
)

echo Running mypy...
mypy src tests
if errorlevel 1 (
    echo Type checking failed! Please fix the issues above.
    exit /b 1
)

echo Running isort...
isort --check-only src tests
if errorlevel 1 (
    echo Import sorting check failed! Please run 'isort src tests' to fix imports.
    exit /b 1
)

REM Run tests with coverage
echo Running tests with coverage...
pytest --cov=src --cov-report=term-missing --cov-report=html

REM Report coverage
echo Coverage report generated in htmlcov\index.html

REM Deactivate virtual environment
deactivate

echo Testing completed!
pause
