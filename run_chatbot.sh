#!/bin/bash

# Shell script to run the Firebolt Data Chat chatbot and open it in a browser
# Usage: ./run_chatbot.sh [port]

set -e

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Default port
PORT="${1:-8000}"

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

# Validate required environment variables
if [ -z "$FIREBOLT_ID" ] || [ -z "$FIREBOLT_SECRET" ]; then
    echo "Error: Firebolt credentials not set in envvars.sh"
    echo "Please set FIREBOLT_ID and FIREBOLT_SECRET"
    exit 1
fi

# Check if chainlit is installed
if ! command -v chainlit &> /dev/null; then
    echo "Error: chainlit not found in virtual environment"
    echo "Please install it with: pip install chainlit"
    exit 1
fi

# Log file path
LOG_FILE="chatbot.log"

echo ""
echo "=========================================="
echo "  Starting Firebolt Data Chat Bot"
echo "=========================================="
echo ""
echo "  Port: $PORT"
echo "  URL: http://localhost:$PORT"
echo "  Log file: $LOG_FILE"
echo ""
echo "Press Ctrl+C to stop the server"
echo ""

# Function to open browser (works on macOS, Linux, and Windows with WSL)
open_browser() {
    local url="$1"
    sleep 2  # Wait a moment for the server to start
    
    if command -v open &> /dev/null; then
        # macOS
        open "$url"
    elif command -v xdg-open &> /dev/null; then
        # Linux
        xdg-open "$url"
    elif command -v start &> /dev/null; then
        # Windows (WSL)
        start "$url"
    else
        echo ""
        echo "Could not automatically open browser. Please navigate to:"
        echo "  $url"
        echo ""
    fi
}

# Cleanup function to kill background processes
cleanup() {
    echo ""
    echo "Shutting down..."
    # Kill chainlit process if it's still running
    if [ -n "$CHAINLIT_PID" ] && kill -0 "$CHAINLIT_PID" 2>/dev/null; then
        kill "$CHAINLIT_PID" 2>/dev/null || true
        wait "$CHAINLIT_PID" 2>/dev/null || true
    fi
    exit 0
}

# Set up signal handlers
trap cleanup SIGINT SIGTERM

# Initialize log file
echo "==========================================" > "$LOG_FILE"
echo "Firebolt Data Chat Bot Log" >> "$LOG_FILE"
echo "Started: $(date)" >> "$LOG_FILE"
echo "Port: $PORT" >> "$LOG_FILE"
echo "==========================================" >> "$LOG_FILE"
echo "" >> "$LOG_FILE"

# Open browser in background after a short delay
# open_browser "http://localhost:$PORT" &

# Run chainlit in background and redirect output to log file
chainlit run data_chat_bot.py --port "$PORT" >> "$LOG_FILE" 2>&1 &
CHAINLIT_PID=$!

# Wait a moment for the log file to be created and have some content
sleep 1

# Tail the log file in the foreground
tail -f "$LOG_FILE"

