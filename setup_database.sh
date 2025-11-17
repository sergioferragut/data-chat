#!/bin/bash

# Shell script to run Firebolt database setup
# Usage:
#   ./setup_database.sh          # Setup database
#   ./setup_database.sh --cleanup # Cleanup all database objects

set -e

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Check if Python virtual environment exists
if [ ! -d "venv312" ]; then
    echo "Error: Python virtual environment 'venv312' not found"
    echo "Please create it with: python3.12 -m venv venv312"
    exit 1
fi

# Check if envvars.sh exists
if [ ! -f "envvars.sh" ]; then
    echo "Error: envvars.sh not found"
    echo "Please create it using demo_setup.sh or manually"
    exit 1
fi

# Activate virtual environment
echo "Activating Python virtual environment..."
source venv312/bin/activate

# Source environment variables
echo "Loading environment variables..."
source envvars.sh

# Run the setup script with all passed arguments
echo "Running database setup script..."
echo ""
python setup_database.py "$@"

