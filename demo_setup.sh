#!/bin/bash

# Demo Setup Script
# This script collects configuration values and generates envvars.sh

set -e

echo "=========================================="
echo "  Firebolt Data Chat Demo Setup"
echo "=========================================="
echo ""

# Function to prompt for input with default value
prompt_with_default() {
    local prompt_text="$1"
    local default_value="$2"
    local var_name="$3"
    local is_secret="${4:-false}"
    
    if [ "$is_secret" = "true" ]; then
        if [ -n "$default_value" ] && [ "$default_value" != "<fill me in>" ]; then
            echo -n "$prompt_text [default: *****]: "
        else
            echo -n "$prompt_text: "
        fi
        read -s user_input
        echo ""
    else
        if [ -n "$default_value" ] && [ "$default_value" != "<fill me in>" ]; then
            echo -n "$prompt_text [default: $default_value]: "
        else
            echo -n "$prompt_text: "
        fi
        read user_input
    fi
    
    if [ -z "$user_input" ]; then
        if [ -n "$default_value" ] && [ "$default_value" != "<fill me in>" ]; then
            eval "$var_name=\"$default_value\""
        else
            eval "$var_name=\"\""
        fi
    else
        eval "$var_name=\"$user_input\""
    fi
}

# Read existing values if envvars.sh exists
if [ -f "envvars.sh" ]; then
    source envvars.sh 2>/dev/null || true
fi

echo "=== AWS Credentials (for Bedrock and general AWS services) ==="
prompt_with_default "AWS Access Key ID" "${AWS_ACCESS_KEY_ID:-}" "AWS_ACCESS_KEY_ID" "true"
prompt_with_default "AWS Secret Access Key" "${AWS_SECRET_ACCESS_KEY:-}" "AWS_SECRET_ACCESS_KEY" "true"
prompt_with_default "AWS Session Token (required if Access Key starts with ASIA)" "${AWS_SESSION_TOKEN:-}" "AWS_SESSION_TOKEN" "true"
echo ""

echo "=== S3 Credentials (for PDF processing - optional, uses AWS credentials if not set) ==="
prompt_with_default "S3 Access Key ID" "${S3_ACCESS_KEY_ID:-}" "S3_ACCESS_KEY_ID" "true"
prompt_with_default "S3 Secret Access Key" "${S3_SECRET_ACCESS_KEY:-}" "S3_SECRET_ACCESS_KEY" "true"
prompt_with_default "S3 Session Token (required if Access Key starts with ASIA)" "${S3_SESSION_TOKEN:-}" "S3_SESSION_TOKEN" "true"
echo ""

echo "=== Firebolt Credentials ==="
prompt_with_default "Firebolt ID" "${FIREBOLT_ID:-}" "FIREBOLT_ID" "true"
prompt_with_default "Firebolt Secret" "${FIREBOLT_SECRET:-}" "FIREBOLT_SECRET" "true"
echo ""

echo "=== Firebolt Configuration ==="
prompt_with_default "Firebolt Engine Name" "${FIREBOLT_ENGINE_NAME:-data_chat_engine}" "FIREBOLT_ENGINE_NAME"
prompt_with_default "Firebolt Database" "${FIREBOLT_DATABASE:-data_chat_demo}" "FIREBOLT_DATABASE"
prompt_with_default "Firebolt Account Name" "${FIREBOLT_ACCOUNT_NAME:-developer}" "FIREBOLT_ACCOUNT_NAME"
prompt_with_default "Firebolt Semantic Index" "${FIREBOLT_SEMANTIC_INDEX:-pdf_semantic_index}" "FIREBOLT_SEMANTIC_INDEX"
prompt_with_default "Firebolt MCP API URL" "${FIREBOLT_MCP_API_URL:-developer-firebolt.api.us-east-1.staging.firebolt.io}" "FIREBOLT_MCP_API_URL"
prompt_with_default "Firebolt LLM Location" "${FIREBOLT_LLM_LOCATION:-llm_api}" "FIREBOLT_LLM_LOCATION"
echo ""

echo "=== AWS Bedrock Configuration ==="
prompt_with_default "Bedrock Model ID" "${BEDROCK_MODEL_ID:-us.anthropic.claude-3-5-sonnet-20241022-v2:0}" "BEDROCK_MODEL_ID"
echo ""

echo "=== S3 Configuration ==="
prompt_with_default "S3 URI for source PDF files" "${S3_SOURCE_PDF_URI:-}" "S3_SOURCE_PDF_URI"
prompt_with_default "S3 URI for target chunked parquet file" "${S3_TARGET_PARQUET_URI:-}" "S3_TARGET_PARQUET_URI"
echo ""

# Validate required fields
if [ -z "$AWS_ACCESS_KEY_ID" ] || [ "$AWS_ACCESS_KEY_ID" = "<fill me in>" ]; then
    echo "Error: AWS_ACCESS_KEY_ID is required"
    exit 1
fi

if [ -z "$AWS_SECRET_ACCESS_KEY" ] || [ "$AWS_SECRET_ACCESS_KEY" = "<fill me in>" ]; then
    echo "Error: AWS_SECRET_ACCESS_KEY is required"
    exit 1
fi

# Check if temporary AWS credentials require session token
if [[ "$AWS_ACCESS_KEY_ID" == ASIA* ]] && ([ -z "$AWS_SESSION_TOKEN" ] || [ "$AWS_SESSION_TOKEN" = "<fill me in>" ]); then
    echo "Error: Temporary AWS credentials detected (starting with ASIA) but AWS_SESSION_TOKEN is missing."
    echo "Temporary credentials require AWS_SESSION_TOKEN to be set."
    exit 1
fi

# Check if temporary S3 credentials require session token (if S3 credentials are set)
if [ -n "$S3_ACCESS_KEY_ID" ] && [ "$S3_ACCESS_KEY_ID" != "<fill me in>" ]; then
    if [[ "$S3_ACCESS_KEY_ID" == ASIA* ]] && ([ -z "$S3_SESSION_TOKEN" ] || [ "$S3_SESSION_TOKEN" = "<fill me in>" ]); then
        echo "Error: Temporary S3 credentials detected (starting with ASIA) but S3_SESSION_TOKEN is missing."
        echo "Temporary credentials require S3_SESSION_TOKEN to be set."
        exit 1
    fi
fi

if [ -z "$FIREBOLT_ID" ] || [ "$FIREBOLT_ID" = "<fill me in>" ]; then
    echo "Error: FIREBOLT_ID is required"
    exit 1
fi

if [ -z "$FIREBOLT_SECRET" ] || [ "$FIREBOLT_SECRET" = "<fill me in>" ]; then
    echo "Error: FIREBOLT_SECRET is required"
    exit 1
fi

# Generate envvars.sh
echo "Generating envvars.sh..."
cat > envvars.sh <<EOF
export AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID"
export AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY"
export AWS_SESSION_TOKEN="$AWS_SESSION_TOKEN"

# S3 Credentials (optional - for PDF processing, uses AWS credentials if not set)
export S3_ACCESS_KEY_ID="${S3_ACCESS_KEY_ID:-}"
export S3_SECRET_ACCESS_KEY="${S3_SECRET_ACCESS_KEY:-}"
export S3_SESSION_TOKEN="${S3_SESSION_TOKEN:-}"

export FIREBOLT_ID="$FIREBOLT_ID"
export FIREBOLT_SECRET="$FIREBOLT_SECRET"

export FIREBOLT_ENGINE_NAME="${FIREBOLT_ENGINE_NAME:-data_chat_engine}"
export FIREBOLT_DATABASE="${FIREBOLT_DATABASE:-data_chat_demo}"
export FIREBOLT_ACCOUNT_NAME="${FIREBOLT_ACCOUNT_NAME:-developer}"
export FIREBOLT_SEMANTIC_INDEX="${FIREBOLT_SEMANTIC_INDEX:-pdf_semantic_index}"

export FIREBOLT_MCP_API_URL="${FIREBOLT_MCP_API_URL:-developer-firebolt.api.us-east-1.staging.firebolt.io}"

export FIREBOLT_LLM_LOCATION="${FIREBOLT_LLM_LOCATION:-llm_api}"

export BEDROCK_MODEL_ID="${BEDROCK_MODEL_ID:-us.anthropic.claude-3-5-sonnet-20241022-v2:0}"

# S3 Configuration
export S3_SOURCE_PDF_URI="${S3_SOURCE_PDF_URI:-}"
export S3_TARGET_PARQUET_URI="${S3_TARGET_PARQUET_URI:-}"
EOF

echo ""
echo "✓ envvars.sh has been generated successfully!"
echo ""
echo "Next steps:"
echo "  1. Review the generated envvars.sh file"
echo "  2. Source it: source envvars.sh"
echo "  3. Run your demo scripts"
echo ""

