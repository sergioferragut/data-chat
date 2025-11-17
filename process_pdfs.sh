#!/bin/bash

# Shell script to process PDFs from S3
# Usage:
#   ./process_pdfs.sh                                    # Use S3 URIs from envvars.sh
#   ./process_pdfs.sh <input_s3_path> <output_s3_path>  # Override S3 URIs

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

# Determine input and output S3 paths
if [ $# -eq 0 ]; then
    # Use S3 URIs from envvars.sh
    if [ -z "$S3_SOURCE_PDF_URI" ]; then
        echo "Error: S3_SOURCE_PDF_URI not set in envvars.sh"
        echo "Please set it in envvars.sh or provide as command-line arguments"
        exit 1
    fi
    
    if [ -z "$S3_TARGET_PARQUET_URI" ]; then
        echo "Error: S3_TARGET_PARQUET_URI not set in envvars.sh"
        echo "Please set it in envvars.sh or provide as command-line arguments"
        exit 1
    fi
    
    INPUT_S3_PATH="$S3_SOURCE_PDF_URI"
    OUTPUT_S3_PATH="$S3_TARGET_PARQUET_URI"
    
    # Ensure output path ends with .parquet or add a default filename
    if [[ ! "$OUTPUT_S3_PATH" =~ \.(parquet|PARQUET)$ ]] && [[ "$OUTPUT_S3_PATH" =~ /$ ]]; then
        OUTPUT_S3_PATH="${OUTPUT_S3_PATH}processed_pdfs.parquet"
    fi
    
    echo "Using S3 paths from envvars.sh:"
    echo "  Input:  $INPUT_S3_PATH"
    echo "  Output: $OUTPUT_S3_PATH"
    echo ""
elif [ $# -eq 2 ]; then
    # Use command-line arguments
    INPUT_S3_PATH="$1"
    OUTPUT_S3_PATH="$2"
    
    echo "Using S3 paths from command-line:"
    echo "  Input:  $INPUT_S3_PATH"
    echo "  Output: $OUTPUT_S3_PATH"
    echo ""
else
    echo "Error: Invalid number of arguments"
    echo "Usage: $0 [input_s3_path] [output_s3_path]"
    echo ""
    echo "Examples:"
    echo "  $0                                    # Use S3 URIs from envvars.sh"
    echo "  $0 s3://bucket/pdfs/ s3://bucket/output.parquet  # Override S3 URIs"
    exit 1
fi

# Validate S3 credentials are set (try S3_* first, then AWS_*)
ACCESS_KEY_ID="${S3_ACCESS_KEY_ID:-$AWS_ACCESS_KEY_ID}"
SECRET_ACCESS_KEY="${S3_SECRET_ACCESS_KEY:-$AWS_SECRET_ACCESS_KEY}"
SESSION_TOKEN="${S3_SESSION_TOKEN:-$AWS_SESSION_TOKEN}"

if [ -z "$ACCESS_KEY_ID" ]; then
    echo "Error: S3 credentials not set in envvars.sh"
    echo "Please set either:"
    echo "  - S3_ACCESS_KEY_ID and S3_SECRET_ACCESS_KEY (preferred for S3 operations)"
    echo "  - AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY (fallback)"
    exit 1
fi

if [ -z "$SECRET_ACCESS_KEY" ]; then
    echo "Error: S3 secret access key not set in envvars.sh"
    echo "Please set either:"
    echo "  - S3_SECRET_ACCESS_KEY (preferred for S3 operations)"
    echo "  - AWS_SECRET_ACCESS_KEY (fallback)"
    exit 1
fi

# Check if temporary credentials require session token
# Temporary AWS credentials (starting with ASIA) require a session token
if [[ "$ACCESS_KEY_ID" == ASIA* ]] && [ -z "$SESSION_TOKEN" ]; then
    echo "Error: Temporary AWS credentials detected (starting with ASIA) but session token is missing."
    echo "Temporary credentials require AWS_SESSION_TOKEN or S3_SESSION_TOKEN to be set."
    echo "Please set one of the following in envvars.sh:"
    echo "  - S3_SESSION_TOKEN (preferred for S3 operations)"
    echo "  - AWS_SESSION_TOKEN (fallback)"
    exit 1
fi

# Run the PDF processing script
echo "Running PDF processing..."
echo ""
python process_pdfs.py "$INPUT_S3_PATH" "$OUTPUT_S3_PATH"

