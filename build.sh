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
VENV_DIR=".venv"
VENV_PYTHON="$VENV_DIR/bin/python3"

if [ ! -d "$VENV_DIR" ]; then
    echo "Creating venv..."
    python3 -m venv "$VENV_DIR"
fi

if [ ! -x "$VENV_PYTHON" ]; then
    echo "Error: virtualenv python not found at $VENV_PYTHON" >&2
    exit 1
fi

# Build and install
DIST_DIR="dist"

echo "Installing build dependencies..."
"$VENV_PYTHON" -m pip install --upgrade pip
"$VENV_PYTHON" -m pip install build scikit-build-core pybind11

echo "Building wheel..."
"$VENV_PYTHON" -m build --outdir "$DIST_DIR"

echo "Installing wheel..."
"$VENV_PYTHON" -m pip install "$DIST_DIR"/*.whl

echo "Done!"
echo "Python artifacts: ./${DIST_DIR}/"
echo "C++ intermediates: ./build/"
