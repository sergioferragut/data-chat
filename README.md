# data-chat
A demo of a chatbot that can have a conversation with an arbitrary set of files by loading the data into structured and vectorized unstructured tables on Firebolt.

The general idea is that you have an S3 bucket with a set of files. CSVs, JSON, PARQUET, etc and a set of PDF documents. 
The system loads all the data into a database and it allows the user to have a conversation with the data.

## Requirements

- **Python 3.12** - This project is tested and works with Python 3.12
- Firebolt account with appropriate credentials
- AWS account with S3 access and Bedrock access
- Docker (for running the Firebolt MCP server)


Some design elements:

Source structured data files must contain column names. 
CURRENT DEMO STATUS: This is currently hard-coded to load flight data and has DDL for corresponding tables and loads. The general idea of the demo is that you could point this at any set of PDFs and structured data and have it figure out the load and enabling of the chat bot.
Name of the files is significant and used for table names.
PDF titles define a knowledge domain (a different table) and content is chunked into the table with embedding index.


This chatbot uses 
- LangChain to process unstructured data. 
- Firebolt as a database for both analytics on structured data and vector search for semantic search of content.
- It responds to users questions based solely on the structured and un-structured data the user has access to.

## Initial Setup

### 1. Create Python Virtual Environment

This project requires Python 3.12. Create a virtual environment:

```bash
python3.12 -m venv venv312
source venv312/bin/activate
```

### 2. Install Dependencies

Install all required Python packages:

```bash
pip install -r requirements.txt
```

### 3. Make Shell Scripts Executable

Make sure all shell scripts are executable:

```bash
chmod +x demo_setup.sh process_pdfs.sh setup_database.sh run_chatbot.sh
```

## Demo Steps

### Step 1: Configure Environment Variables

Use the interactive setup script to configure your environment variables:

```bash
./demo_setup.sh
```

This script will:
- Prompt you for AWS credentials (Access Key ID, Secret Access Key, Session Token if needed)
- Prompt you for S3 credentials (optional, uses AWS credentials if not set)
- Prompt you for Firebolt credentials (Client ID and Secret)
- Prompt you for Firebolt configuration (Engine Name, Database, Account Name, etc.)
- Prompt you for S3 URIs for source PDFs and target parquet file
- Generate an `envvars.sh` file with all your configuration

**Note:** The script will use existing values from `envvars.sh` as defaults if the file already exists. For sensitive fields (credentials), it will show `*****` as the default if a value exists.

**Important:** 
- If your AWS Access Key ID starts with `ASIA`, you must provide a Session Token (temporary credentials)
- The generated `envvars.sh` file is automatically added to `.gitignore` to prevent committing credentials

### Step 2: Process PDFs from S3

Process PDF files from an S3 bucket and create a parquet file with page-level content:

CURRENT DEMO STATUS: This step has been executed, the chunked for travel data example, so it is not needed if you are just demoing the database portion in the next step.

```bash
# Use S3 URIs from envvars.sh
./process_pdfs.sh

# Or override S3 URIs via command-line arguments
./process_pdfs.sh s3://your-bucket/pdfs/ s3://your-bucket/output.parquet

# TO USE STOCK AIR TRAVEL EXAMPLE USE 
./process_pdfs.sh s3://firebolt-publishing-public/faa-pdfs/pdfs/ s3://firebolt-publishing-public/faa-pdfs/chunked/
```

The script will:
- Activate the Python virtual environment automatically
- Load environment variables from `envvars.sh`
- List all PDF files in the S3 input path
- Extract text from each page of each PDF
- Create a parquet file with columns: `filename`, `page_num`, `page_content`
- Upload the parquet file to the S3 output path

**Note:** The script validates that S3 credentials are set and that session tokens are provided for temporary credentials (starting with `ASIA`).

### Step 3: Set Up Firebolt Database

Set up the Firebolt database, tables, external tables, semantic index, and LLM location:

```bash
# Set up the database
./setup_database.sh

# Or clean up all database objects (including database and engine)
./setup_database.sh --cleanup
```

The script will:
- Activate the Python virtual environment automatically
- Load environment variables from `envvars.sh`
- Read the `setup_ddl.sql` file
- Parameterize the SQL with values from your environment variables:
  - Database name from `FIREBOLT_DATABASE`
  - Engine name from `FIREBOLT_ENGINE_NAME`
  - AWS credentials for the LLM location from `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`
  - S3 URI for the external table from `S3_TARGET_PARQUET_URI`
- Execute all SQL statements to create:
  - Database
  - External table pointing to your S3 parquet file
  - Semantic knowledge table
  - Semantic index with vector search
  - LLM location for embeddings

**Note:** The `setup_ddl.sql` file uses placeholders (`<fill me in>`) for credentials, which are automatically replaced by the script with values from your `envvars.sh` file.

### Step 4: Run the Chatbot

Start the Chainlit chatbot interface:

```bash
# Start on default port 8000
./run_chatbot.sh

# Or specify a custom port
./run_chatbot.sh 8080
```

The script will:
- Activate the Python virtual environment automatically
- Load environment variables from `envvars.sh`
- Validate that Firebolt credentials are set
- Start the Chainlit server
- Write logs to `chatbot.log`
- Tail the log file in real-time
- Automatically open the chatbot URL in your browser (on macOS/Linux)

**Note:** 
- The chatbot will start on `http://localhost:8000` (or your specified port)
- Press `Ctrl+C` to stop the server
- Logs are written to `chatbot.log` for debugging

The chatbot will:
- Connect to Firebolt using your credentials
- Use the Firebolt MCP server for SQL query execution
- Use the Firebolt vector store for PDF document semantic search
- Allow you to ask questions about your PDF content and structured data

Access the UI in your browser at `http://localhost:8000` and start chatting with your data!

### Example Questions

Once the chatbot is running, you can ask questions about your data. Here are some example questions you might try:

**PDF Document Questions (using semantic search):**
- "What are the main topics covered in the PDF documents?"
- "Summarize the key findings from the documents"
- "What regulations are mentioned in the FAA documents?"
- "Find information about flight delays and their causes"

**Structured Data Questions (using SQL queries):**
- "How many flights were delayed in January?"
- "What is the average delay time by carrier?"
- "Show me the top 10 airports with the most cancellations"
- "What percentage of flights were on-time last month?"

**Combined Questions:**
- "Based on the documents, what are the main causes of flight delays, and how does this compare to the actual delay data?"
- "What do the regulations say about weather delays, and how many weather-related delays occurred?"

The chatbot will automatically:
- Use semantic search for questions about PDF content
- Use SQL queries for structured data questions
- Combine both approaches when appropriate

## Troubleshooting

### Test Firebolt Connection

Test your Firebolt connection and credentials:

```bash
source venv312/bin/activate
source envvars.sh
python test_firebolt_simple.py
```

This will verify:
- Authentication works
- Connection to Firebolt succeeds
- Basic queries execute successfully

### Test Firebolt Vector Store

Test the Firebolt vector store implementation:

```bash
source venv312/bin/activate
source envvars.sh
python test_firebolt_vectorstore_sql.py
```

This will test:
- Vector store initialization
- Similarity search functionality
- Embedding generation using Firebolt's `AI_EMBED_TEXT`

