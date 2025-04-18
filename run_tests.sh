#!/bin/bash

# Exit on error
set -e

echo "Setting up E-commerce Analytics testing environment..."

# Check Python version
python_version=$(python --version 2>&1 | awk '{print $2}')
required_version="3.8.10"

if ! command -v python &> /dev/null; then
    echo "Python is not installed!"
    exit 1
fi

printf "Checking Python version... "
if [ "$(printf '%s\n' "$required_version" "$python_version" | sort -V | head -n1)" = "$required_version" ]; then 
    echo "OK (${python_version})"
else 
    echo "ERROR: Python ${required_version} or higher is required (found ${python_version})"
    exit 1
fi

# Create and activate virtual environment
echo "Creating virtual environment..."
if [ ! -d "venv" ]; then
    python -m venv venv
fi

# Activate virtual environment
if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "win32" ]]; then
    source venv/Scripts/activate
else
    source venv/bin/activate
fi

# Install dependencies
echo "Installing dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

# Run style checks
echo "Running style checks..."
echo "Running black..."
black --check src tests
echo "Running flake8..."
flake8 src tests
echo "Running mypy..."
mypy src tests
echo "Running isort..."
isort --check-only src tests

# Run tests with coverage
echo "Running tests with coverage..."
pytest --cov=src --cov-report=term-missing --cov-report=html

# Generate coverage report
echo "Coverage report generated in htmlcov/index.html"

# Deactivate virtual environment
deactivate

echo "Testing completed!"
