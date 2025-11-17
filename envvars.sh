export AWS_ACCESS_KEY_ID="<fill me in>"
export AWS_SECRET_ACCESS_KEY="<fill me in>"
export AWS_SESSION_TOKEN="<fill me in>"

# S3 Credentials (optional - for PDF processing, uses AWS credentials if not set)
export S3_ACCESS_KEY_ID="<fill me in>"
export S3_SECRET_ACCESS_KEY="<fill me in>"
export S3_SESSION_TOKEN="<fill me in>"

export FIREBOLT_ID="<fill me in>"
export FIREBOLT_SECRET="<fill me in>"

export FIREBOLT_ENGINE_NAME="data_chat_engine"
export FIREBOLT_DATABASE="data_chat_demo"
export FIREBOLT_ACCOUNT_NAME="developer"
export FIREBOLT_SEMANTIC_INDEX="pdf_semantic_index"

export FIREBOLT_MCP_API_URL="developer-firebolt.api.us-east-1.staging.firebolt.io"

export FIREBOLT_LLM_LOCATION="llm_api"

export BEDROCK_MODEL_ID="us.anthropic.claude-3-5-sonnet-20241022-v2:0"

# S3 Configuration
export S3_SOURCE_PDF_URI="s3://firebolt-publishing-public/faa-pdfs/pdfs/"
export S3_TARGET_PARQUET_URI="s3://firebolt-publishing-public/faa-pdfs/chunked/"
