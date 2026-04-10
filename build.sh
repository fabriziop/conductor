#!/bin/bash
set -euo pipefail

# Error handler: print error message and exit
trap 'echo "Error: build failed at line $LINENO" >&2; exit 1' ERR

# Check for required tools
if ! command -v python3 &> /dev/null; then
    echo "Error: python3 not found in PATH" >&2
    exit 1
fi

# Auto-create and activate venv if needed
if [ ! -d .venv ]; then
    echo "Creating venv..."
    python3 -m venv .venv
fi

# Activate venv
source .venv/bin/activate

# Build and install
echo "Installing build dependencies..."
python -m pip install build scikit-build-core pybind11

echo "Building wheel..."
python -m build

echo "Installing wheel..."
python -m pip install dist/*.whl

echo "Done!"
