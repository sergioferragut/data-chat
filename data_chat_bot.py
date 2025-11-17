import chainlit as cl
import os
import sys
import urllib
import traceback as tb
import logging
from chainlit.mcp import McpConnection
from langchain_core.messages import AIMessageChunk, HumanMessage
from langchain_core.runnables import RunnableConfig
from langchain_core.tools import create_retriever_tool
from langchain_mcp_adapters.tools import load_mcp_tools
from langgraph.graph.state import CompiledStateGraph
from langgraph.prebuilt import create_react_agent
from langchain_aws import ChatBedrockConverse
from langchain_community.vectorstores.firebolt import Firebolt, FireboltSettings
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from typing import cast
from firebolt.client.auth import ClientCredentials
from firebolt.db import connect

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

firebolt_id = os.getenv('FIREBOLT_ID')
firebolt_secret_raw = os.getenv('FIREBOLT_SECRET')
firebolt_secret = urllib.parse.quote_plus(firebolt_secret_raw) if firebolt_secret_raw else None
aws_key = os.getenv("AWS_ACCESS_KEY_ID") or os.getenv("AWS_KEY")
aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY") or os.getenv("AWS_SECRET")
aws_session_token = os.getenv("AWS_SESSION_TOKEN")

def get_database_schema(database_name: str, engine_name: str, account_name: str, api_endpoint: str = None) -> str:
	"""
	Query the database to get a list of tables and their columns.
	Returns a formatted string describing the database schema.
	"""
	try:
		# Create Firebolt connection
		auth = ClientCredentials(
			client_id=firebolt_id,
			client_secret=firebolt_secret_raw
		)
		
		connection_params = {
			"auth": auth,
			"engine_name": engine_name,
			"database": database_name,
			"account_name": account_name
		}
		
		if api_endpoint:
			connection_params["api_endpoint"] = api_endpoint
		
		connection = connect(**connection_params)
		cursor = connection.cursor()
		
		# Get list of tables (excluding system tables and internal tables)
		cursor.execute("""
			SELECT table_name 
			FROM information_schema.tables 
			WHERE table_schema = 'public' 
			AND table_type = 'BASE TABLE'
			AND table_name NOT LIKE 'pg_%'
			AND table_name NOT IN ('ext_pdf_content', 'pdf_semantic_knowledge', 'pdf_semantic_index')
			ORDER BY table_name
		""")
		
		tables = cursor.fetchall()
		
		if not tables:
			return "No user tables found in the database."
		
		schema_info = f"Available tables in the {database_name} database:\n\n"
		
		# For each table, get its columns
		for (table_name,) in tables:
			try:
				# Validate table name contains only safe characters (alphanumeric and underscore)
				# Since table names come from information_schema, they should be safe, but validate anyway
				if not all(c.isalnum() or c == '_' for c in table_name):
					logger.warning(f"Skipping table with unsafe characters: {table_name}")
					continue
				
				# Use string formatting - safe because table_name comes from database metadata
				# and we've validated it contains only safe characters
				cursor.execute(f"""
					SELECT column_name, data_type, is_nullable
					FROM information_schema.columns
					WHERE table_schema = 'public'
					AND table_name = '{table_name}'
					ORDER BY ordinal_position
				""")
				
				columns = cursor.fetchall()
				
				if columns:
					schema_info += f"Table: {table_name}\n"
					schema_info += "  Columns:\n"
					for col_name, data_type, is_nullable in columns:
						nullable = "NULL" if is_nullable == "YES" else "NOT NULL"
						schema_info += f"    - {col_name} ({data_type}, {nullable})\n"
					schema_info += "\n"
			except Exception as e:
				logger.warning(f"Could not get columns for table {table_name}: {e}")
				schema_info += f"Table: {table_name} (columns not available)\n\n"
		
		connection.close()
		return schema_info
		
	except Exception as e:
		logger.error(f"Error getting database schema: {e}")
		logger.error(tb.format_exc())
		return f"Could not retrieve database schema: {str(e)}"


async def cleanup_old_containers(max_age_minutes=30):
	"""Clean up old MCP containers that are no longer in use."""
	import subprocess
	import time
	
	try:
		# Get all firebolt-mcp containers
		result = subprocess.run(
			["docker", "ps", "-a", "--filter", "name=firebolt-mcp-", "--format", "{{.Names}} {{.Status}} {{.CreatedAt}}"],
			capture_output=True,
			text=True,
			timeout=5
		)
		
		if not result.stdout.strip():
			return
		
		cleaned = 0
		for line in result.stdout.strip().split('\n'):
			if not line.strip():
				continue
			
			parts = line.split(' ', 2)
			if len(parts) < 3:
				continue
			
			container_name = parts[0]
			status = parts[1]
			created_str = parts[2] if len(parts) > 2 else ""
			
			# Remove stopped containers immediately
			if "Exited" in status or "Dead" in status:
				try:
					subprocess.run(
						["docker", "rm", "-f", container_name],
						capture_output=True,
						timeout=2
					)
					cleaned += 1
					logger.info(f"Cleaned up stopped container: {container_name}")
				except:
					pass
		
		if cleaned > 0:
			logger.info(f"Cleaned up {cleaned} old/stopped MCP containers")
	except Exception as e:
		logger.warning(f"Error during container cleanup: {e}")


async def get_or_create_agent():
	"""Get existing agent or create a new one. Returns agent and components."""
	# Check if agent already exists
	agent = cl.user_session.get('agent')
	if agent:
		logger.info("Reusing existing agent from session")
		return agent, None
	
	# Check if initialization is already in progress
	if cl.user_session.get('initializing'):
		logger.warning("Initialization already in progress, waiting...")
		# Wait and check again in a loop with timeout
		import asyncio
		max_wait_time = 30  # Maximum wait time in seconds
		wait_interval = 1   # Check every second
		waited = 0
		while waited < max_wait_time:
			await asyncio.sleep(wait_interval)
			waited += wait_interval
			agent = cl.user_session.get('agent')
			if agent:
				return agent, None
			# Check if initialization is still in progress
			if not cl.user_session.get('initializing'):
				# Initialization completed (or failed), break and try again
				break
		# If we've waited too long, raise an error
		if waited >= max_wait_time:
			raise TimeoutError("Agent initialization timed out after waiting for another initialization to complete")
	
	# Mark as initializing
	cl.user_session.set('initializing', True)
	try:
		# Initialize model
		# Try Claude Opus first, fallback to Claude Sonnet if marketplace access is denied
		# Claude Sonnet 3.5 doesn't require Marketplace subscription
		model_id = os.getenv("BEDROCK_MODEL_ID", "us.anthropic.claude-3-5-sonnet-20241022-v2:0")
		model_kwargs = {
			"model_id": model_id,
			"region_name": "us-east-1",
			"aws_access_key_id": aws_key,
			"aws_secret_access_key": aws_secret
		}
		# Add session token if available (required for temporary credentials)
		if aws_session_token:
			model_kwargs["aws_session_token"] = aws_session_token
		
		model = ChatBedrockConverse(**model_kwargs)

		# Docker parameters for Firebolt MCP server
		# Each session needs its own container because Docker containers with -i (interactive)
		# can only have ONE stdio connection at a time. Multiple sessions = multiple containers.
		import subprocess
		
		# Use session ID to create unique container name per session
		session_id_short = cl.context.session.id[:12]
		container_name = f"firebolt-mcp-{session_id_short}"
		
		# Check if we already have a container for this session
		existing_container = cl.user_session.get('mcp_container_name')
		if existing_container:
			# Check if that container is still running
			try:
				result = subprocess.run(
					["docker", "ps", "--filter", f"name={existing_container}", "--format", "{{.Names}}"],
					capture_output=True,
					text=True,
					timeout=2
				)
				if existing_container in result.stdout:
					# Container is running, reuse it
					container_name = existing_container
					logger.info(f"Reusing existing container for session: {container_name}")
				else:
					# Container exists but is stopped, remove it
					subprocess.run(
						["docker", "rm", "-f", existing_container],
						capture_output=True,
						timeout=2
					)
					logger.info(f"Removed stopped container {existing_container}, creating new one")
			except Exception as e:
				logger.warning(f"Error checking existing container: {e}")
		
		# Clean up any old container with this name if it exists but is stopped
		try:
			subprocess.run(
				["docker", "rm", "-f", container_name],
				capture_output=True,
				timeout=2
			)
		except:
			pass  # Ignore errors if container doesn't exist
		
		# Final check: if container name already exists and is running, use a unique suffix
		# This handles race conditions where multiple initializations happen simultaneously
		try:
			final_check = subprocess.run(
				["docker", "ps", "--filter", f"name={container_name}", "--format", "{{.Names}}"],
				capture_output=True,
				text=True,
				timeout=2
			)
			if container_name in final_check.stdout:
				# Container already running (race condition), add unique suffix
				import time
				container_name = f"{container_name}-{int(time.time() * 1000)}"
				logger.warning(f"Container name conflict, using unique name: {container_name}")
		except:
			pass
		
		# Build Docker command with Firebolt credentials
		# The MCP server uses FIREBOLT_MCP_ENVIRONMENT to specify the environment
		firebolt_api_url = os.getenv("FIREBOLT_MCP_API_URL")
		# Determine if we're using staging environment
		firebolt_mcp_environment = None
		if firebolt_api_url and "staging" in firebolt_api_url.lower():
			firebolt_mcp_environment = "staging.firebolt.io"
			logger.info("Detected staging environment, setting FIREBOLT_MCP_ENVIRONMENT=staging.firebolt.io")
		# If not staging, don't set it (defaults to production)
		
		# Get account name from environment
		firebolt_account_name = os.getenv("FIREBOLT_ACCOUNT_NAME", "developer")
		
		docker_args = [
			"run", "-i", "--name", container_name,
			"-e", f"FIREBOLT_MCP_CLIENT_ID={firebolt_id}",
			"-e", f"FIREBOLT_MCP_CLIENT_SECRET={firebolt_secret_raw}",
			"-e", "FIREBOLT_MCP_DISABLE_RESOURCES=true"
		]
		
		# Add environment setting if staging (the MCP server expects FIREBOLT_MCP_ENVIRONMENT)
		if firebolt_mcp_environment:
			docker_args.extend(["-e", f"FIREBOLT_MCP_ENVIRONMENT={firebolt_mcp_environment}"])
			logger.info(f"Setting FIREBOLT_MCP_ENVIRONMENT={firebolt_mcp_environment} for MCP server")
		
		# Add account name if specified (MCP server may support FIREBOLT_MCP_ACCOUNT_NAME)
		# If not supported, it will be ignored, but worth trying
		if firebolt_account_name:
			docker_args.extend(["-e", f"FIREBOLT_MCP_ACCOUNT_NAME={firebolt_account_name}"])
			logger.info(f"Setting FIREBOLT_MCP_ACCOUNT_NAME={firebolt_account_name} for MCP server")
		
		docker_args.append("ghcr.io/firebolt-db/mcp-server:0.4.0")
		
		server_params = StdioServerParameters(
			command="docker",
			args=docker_args
		)
		
		# Store container name for cleanup
		cl.user_session.set('mcp_container_name', container_name)
		
		# Log container creation (only if it's a new one, not a reuse)
		if not existing_container or existing_container != container_name:
			logger.info(f"Creating container for session {session_id_short}: {container_name}")

		# Use async context manager properly to keep connection alive
		# We'll use a wrapper to keep the context managers alive
		stdio_ctx = stdio_client(server_params)
		read, write = await stdio_ctx.__aenter__()
		
		# Create and initialize session
		session_ctx = ClientSession(read, write)
		session = await session_ctx.__aenter__()
		
		try:
			await session.initialize()
		except Exception as e:
			logger.error(f"Failed to initialize MCP session: {e}")
			# Clean up on error
			try:
				await session_ctx.__aexit__(None, None, None)
			except:
				pass
			try:
				await stdio_ctx.__aexit__(None, None, None)
			except:
				pass
			# Clean up Docker container on error
			container_name = cl.user_session.get('mcp_container_name')
			if container_name:
				try:
					subprocess.run(
						["docker", "rm", "-f", container_name],
						capture_output=True,
						timeout=2
					)
					logger.info(f"Cleaned up container on error: {container_name}")
				except:
					pass
			raise

		# Load MCP tools into LangChain format
		mcp_tools = await load_mcp_tools(session)
		
		# Initialize Firebolt vector store for PDF document search
		logger.info("Initializing Firebolt vector store for PDF document search...")
		vector_store = None
		try:
			# Determine API endpoint - use staging endpoint if MCP API URL indicates staging
			firebolt_api_url = os.getenv("FIREBOLT_MCP_API_URL", "")
			api_endpoint = None
			if firebolt_api_url and "staging" in firebolt_api_url.lower():
				api_endpoint = "https://api.staging.firebolt.io"
			elif firebolt_api_url:
				base_url = firebolt_api_url.split("?")[0]
				if not base_url.startswith("http"):
					api_endpoint = f"https://{base_url}"
				else:
					api_endpoint = base_url
			
			vector_store_config = FireboltSettings(
				firebolt_id=firebolt_id,
				firebolt_secret=firebolt_secret_raw,
				engine_name=os.getenv("FIREBOLT_ENGINE_NAME", "data_chat_engine"),
				database=os.getenv("FIREBOLT_DATABASE", "data_chat_demo"),
				account_name=os.getenv("FIREBOLT_ACCOUNT_NAME", "developer"),
				semantic_index=os.getenv("FIREBOLT_SEMANTIC_INDEX", "pdf_semantic_index"),
				llm_location=os.getenv("FIREBOLT_LLM_LOCATION", "llm_api"),
				embedding_model="amazon.titan-embed-text-v2:0",
				embedding_dimensions=256,
				api_endpoint=api_endpoint
			)
			
			vector_store = Firebolt(config=vector_store_config)
			logger.info("Firebolt vector store initialized successfully")
			
			# Create a retriever tool from the vector store
			pdf_retriever_tool = create_retriever_tool(
				vector_store.as_retriever(search_kwargs={"k": 10}),
				"pdf_document_search",
				"Search through PDF documents using semantic similarity. Use this tool when the user asks about information that might be in PDF documents, such as regulations, requirements, procedures, or any content that was loaded from PDF files."
			)
			
			# Combine MCP tools with the PDF retriever tool
			tools = list(mcp_tools) + [pdf_retriever_tool]
			logger.info(f"Added PDF document search tool. Total tools: {len(tools)}")
		except Exception as e:
			logger.error(f"Failed to initialize Firebolt vector store: {e}")
			logger.warning("Continuing without PDF document search capability")
			# Continue with just MCP tools if vector store fails
			tools = mcp_tools

		# Get database schema information
		logger.info("Fetching database schema information...")
		database_name = os.getenv("FIREBOLT_DATABASE", "data_chat_demo")
		engine_name = os.getenv("FIREBOLT_ENGINE_NAME", "data_chat_engine")
		account_name = os.getenv("FIREBOLT_ACCOUNT_NAME", "developer")
		
		# Determine API endpoint
		firebolt_api_url = os.getenv("FIREBOLT_MCP_API_URL", "")
		api_endpoint = None
		if firebolt_api_url and "staging" in firebolt_api_url.lower():
			api_endpoint = "https://api.staging.firebolt.io"
		elif firebolt_api_url:
			base_url = firebolt_api_url.split("?")[0]
			if not base_url.startswith("http"):
				api_endpoint = f"https://{base_url}"
			else:
				api_endpoint = base_url
		
		schema_info = get_database_schema(database_name, engine_name, account_name, api_endpoint)
		logger.info("Database schema retrieved successfully")

		# Create the agent prompt with schema information
		agent_prompt = f"""
            You are a helpful assistant that translates natural language questions into data insights. 
            You are working on top of the PostgreSQL-compliant data warehouse Firebolt. 
            
            You have access to two types of tools:
            1. Firebolt SQL tools thru the mcp server - Use these to query structured data tables in the {database_name} database.
            2. Reference Knowledge tool called pdf_document_search - Use this to find information that might help answer the user's question.
            
            Database Schema:
            {schema_info}
            
            Important guidelines:
            - Only query tables that exist in the {database_name} database (see schema above).
            - Use the exact table and column names as shown in the schema.
            - If a query fails, don't show the error message to the user. Instead, try to fix it yourself by checking the available tables and columns in the schema above.
            - Do not query ext_pdf_content, pdf_semantic_knowledge, or pdf_semantic_index tables directly. Use the pdf_document_search tool instead.
            - When writing SQL queries, ensure column names match exactly as shown in the schema (case-sensitive).
            """
		
		# Log the complete prompt to help with debugging
		logger.info("=" * 80)
		logger.info("AGENT PROMPT (with database schema):")
		logger.info("=" * 80)
		logger.info(agent_prompt)
		logger.info("=" * 80)

		# Create the agent with schema information in the prompt
		agent = create_react_agent(model, tools, prompt=agent_prompt)

		# Store components in session (store context managers to keep them alive)
		cl.user_session.set('stdio_ctx', stdio_ctx)
		cl.user_session.set('session_ctx', session_ctx)
		cl.user_session.set('mcp_session', session)
		cl.user_session.set('mcp_tools', tools)
		cl.user_session.set('model', model)
		cl.user_session.set('agent', agent)
		# Store vector store if it was successfully initialized
		if vector_store is not None:
			cl.user_session.set('vector_store', vector_store)
		
		logger.info(f"Agent created successfully with container: {container_name}")
		return agent, {'stdio_ctx': stdio_ctx, 'session_ctx': session_ctx, 'session': session}
	finally:
		# Clear initialization flag
		cl.user_session.set('initializing', False)


@cl.on_chat_start
async def on_chat_start():
	cl.user_session.set('chat_messages', [])
	
	# Clean up old containers on first session (only once to avoid overhead)
	# Use a simple flag to prevent multiple cleanups
	if not hasattr(on_chat_start, '_cleanup_done'):
		await cleanup_old_containers()
		on_chat_start._cleanup_done = True
	
	# Send a welcome message immediately
	await cl.Message(
		content="Hello! I'm your data assistant. I can help you query your Firebolt database and search through PDF documents. What would you like to know?"
	).send()
	
	# Initialize session in background (non-blocking)
	# This allows the UI to show immediately while initialization happens
	try:
		await get_or_create_agent()
		await cl.Message(
			content="✓ Connection established. Ready to answer your questions!"
		).send()
	except Exception as e:
		logger.error(f"Failed to initialize session: {e}")
		logger.error(tb.format_exc())
		await cl.Message(
			content=f"⚠️ Error initializing connection: {str(e)}\n\nYou can still try asking a question - the connection will be retried."
		).send()


@cl.on_message
async def on_message(message: cl.Message):
	# Get or initialize agent (lazy initialization)
	try:
		agent, _ = await get_or_create_agent()
	except Exception as e:
		logger.error(f"Failed to get or create agent: {e}")
		logger.error(tb.format_exc())
		error_msg = str(e)
		if "Connection closed" in error_msg or "closed" in error_msg.lower():
			# Connection was closed, clear the session and retry
			cl.user_session.set('agent', None)
			cl.user_session.set('mcp_session', None)
			cl.user_session.set('stdio_ctx', None)
			cl.user_session.set('session_ctx', None)
			try:
				agent, _ = await get_or_create_agent()
			except Exception as retry_error:
				await cl.Message(
					content=f"Error: Connection failed. {str(retry_error)}\n\nPlease try refreshing the page."
				).send()
				return
		else:
			await cl.Message(
				content=f"Error: Failed to initialize connection. {error_msg}"
			).send()
			return
	
	if not agent:
		await cl.Message(content='Error: Chat model not initialized. Please refresh the page.').send()
		return

	config = RunnableConfig(configurable={'thread_id': cl.context.session.id})
	agent = cast(CompiledStateGraph, agent)

	# Use Chainlit's callback handler if available, otherwise use a simple handler
	try:
		cb = cl.AsyncLangchainCallbackHandler()
	except (AttributeError, ImportError):
		# Fallback: create a simple callback handler
		from langchain_core.callbacks import AsyncCallbackHandler
		cb = AsyncCallbackHandler()

	try:
		# Create a message for streaming
		response_message = cl.Message(content="")
		response_content = ""

		# Stream the response using the LangChain callback handler
		# Update the config to include callbacks
		config['callbacks'] = [cb]
		# Convert message to proper format for LangChain
		logger.info(f"Processing message: {message.content[:50]}...")
		message_count = 0
		
		# Filter out internal MCP server details
		def should_include_content(content_str):
			"""Filter out MCP server internals and tool call details."""
			if not content_str:
				return False
			content_lower = content_str.strip().lower()
			
			# Filter out content that is clearly tool call descriptions
			# These patterns indicate internal tool execution details, not user-facing content
			exclude_patterns = [
				'[called ',  # "[Called tool_name with parameters: ...]"
				'[tool output:',  # "[Tool output: ...]"
				'[tool:',  # "[Tool: ...]"
				'[function:',  # "[Function: ...]"
				'executing tool',
				'calling tool',
				'tool_result',
				'tool_use',
			]
			
			# Only exclude if content starts with or is clearly a tool call description
			# This prevents filtering legitimate content that happens to mention these terms
			for pattern in exclude_patterns:
				if content_lower.startswith(pattern) or f'[{pattern}' in content_lower:
					return False
			
			# Filter out very short content that's just tool metadata
			if len(content_str.strip()) < 10 and any(word in content_lower for word in ['tool', 'function', 'mcp']):
				return False
				
			return True
		
		async for msg, metadata in agent.astream(
			{'messages': [HumanMessage(content=message.content)]},
			stream_mode='messages',
			config=config,
		):
			message_count += 1
			# Log all message types for debugging (but don't show to user)
			msg_type = type(msg).__name__
			msg_content = getattr(msg, 'content', 'N/A')
			logger.info(f"Message #{message_count}: type={msg_type}, content_preview={str(msg_content)[:100] if msg_content else 'None'}")
			
			# Log ToolMessage errors to help identify if errors come from MCP server tools
			if msg_type == 'ToolMessage':
				tool_name = getattr(msg, 'name', 'unknown')
				tool_content = str(msg_content) if msg_content else 'None'
				logger.info(f"ToolMessage from {tool_name}: {tool_content[:500]}")
				# Check if this is an error from a tool
				if 'error' in tool_content.lower() or 'failed' in tool_content.lower() or 'does not exist' in tool_content.lower():
					logger.error(f"Tool error detected from {tool_name}: {tool_content[:1000]}")
			
			# Skip ToolMessage and ToolCall types - these are internal (but we log them above)
			if msg_type in ['ToolMessage', 'ToolCall', 'ToolCallChunk']:
				continue
			
			# Handle AIMessageChunks with text content for streaming
			if isinstance(msg, AIMessageChunk):
				if msg.content:
					# If content is a string, filter and stream it
					if isinstance(msg.content, str):
						if should_include_content(msg.content):
							response_content += msg.content
							await response_message.stream_token(msg.content)
					# If content is a list with dictionaries that have text
					elif (
						isinstance(msg.content, list)
						and len(msg.content) > 0
						and isinstance(msg.content[0], dict)
						and msg.content[0].get('type') == 'text'
						and 'text' in msg.content[0]
					):
						text = msg.content[0]["text"]
						if should_include_content(text):
							response_content += text
							await response_message.stream_token(text)
			
			# Also handle regular AIMessage (non-chunk)
			elif hasattr(msg, 'content') and msg.content:
				if isinstance(msg.content, str):
					if should_include_content(msg.content):
						response_content += msg.content
						await response_message.stream_token(msg.content)

		# Send the complete message (update if we have content)
		if response_content:
			await response_message.update()
		else:
			# If no content was streamed, send a message indicating the agent is processing
			await cl.Message(
				content="I'm processing your request. If you don't see a response, the agent may be waiting for tool execution."
			).send()
			logger.warning("No response content was generated by the agent")

	except Exception as e:
		# Error handling
		error_str = str(e)
		logger.error(f"Error in message handling: {e}")
		logger.error(tb.format_exc())
		
		# Check for specific error types and provide helpful guidance
		if ("ExpiredTokenException" in error_str or "expired" in error_str.lower()) and "token" in error_str.lower():
			await cl.Message(
				content="""**AWS Session Token Expired**

Your AWS session token has expired. Temporary AWS credentials typically expire after 1-12 hours.

**To fix this:**

1. **Get new AWS credentials:**
   - If using AWS SSO: Run `aws sso login` in your terminal
   - If using temporary credentials: Get new credentials from your AWS administrator
   - If using AWS CLI profiles: Run `aws configure` or refresh your session

2. **Update your credentials:**
   - Update the `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_SESSION_TOKEN` in your `envvars.sh` file
   - Or export them in your terminal before starting the chatbot

3. **Restart the chatbot:**
   - Stop the current chatbot process
   - Source the updated `envvars.sh` file
   - Restart the chatbot

**Note:** If you're using permanent AWS credentials (IAM user), you won't have a session token and this error shouldn't occur.

Error details: """ + error_str[:200]
			).send()
		elif "AccessDeniedException" in error_str and "aws-marketplace" in error_str.lower():
			err_msg = cl.Message(
				content="""**AWS Marketplace Access Error**

The Claude Opus model requires AWS Marketplace subscription. 

**Options:**
1. Subscribe to Claude Opus in AWS Marketplace
2. Use a different model by setting `BEDROCK_MODEL_ID` environment variable:
   - `us.anthropic.claude-sonnet-4-20250514-v1:0` (Claude Sonnet 4)
   - `us.anthropic.claude-3-5-sonnet-20241022-v2:0` (Claude 3.5 Sonnet)
   - `us.anthropic.claude-3-opus-20240229-v1:0` (Claude 3 Opus - may also need subscription)

**To fix:** Contact your AWS administrator to:
- Subscribe to the model in AWS Marketplace, OR
- Update IAM policy to allow `aws-marketplace:Subscribe` action

Error details: """ + error_str[:200]
			)
		elif "Invalid domain" in error_str and "firebolt" in error_str.lower():
			err_msg = cl.Message(
				content="""**Firebolt Authentication Error**

The Firebolt client ID doesn't match the authentication domain.

**Possible causes:**
1. Client ID is registered for a different Firebolt environment/domain
2. Incorrect Firebolt credentials in environment variables
3. Client ID and secret don't match

**To fix:**
1. Verify your Firebolt credentials are correct:
   - Check `FIREBOLT_ID` and `FIREBOLT_SECRET` in `envvars.sh`
   - Ensure they match your Firebolt account
2. If using a custom Firebolt domain, set `FIREBOLT_MCP_API_URL` environment variable
3. Contact Firebolt support if credentials are correct but still failing

Error details: """ + error_str[:300]
			)
		elif "relation" in error_str.lower() and "does not exist" in error_str.lower():
			# This is likely a Firebolt SQL error from MCP server
			err_msg = cl.Message(
				content=f"""**Firebolt SQL Error: Relation Not Found**

The query is trying to access a table or index that doesn't exist in the database.

**Error details:** {error_str[:500]}

**Possible causes:**
1. The semantic index `pdf_semantic_index` doesn't exist in the `data_chat_demo` database
2. The query is trying to reference `pdf_semantic_index` as a table instead of using `vector_search()` function
3. The table/index name is misspelled or doesn't exist in the current database

**To fix:**
1. Verify the semantic index exists by running the `setup_ddl.sql` script
2. Ensure queries use `vector_search(INDEX pdf_semantic_index, ...)` instead of querying `pdf_semantic_index` directly
3. Check that you're connected to the correct database (`data_chat_demo`)

**Note:** This error is coming from the MCP server when executing a SQL query, not from the vector store code.

Check server logs for the full SQL query that caused this error.
"""
			)
		else:
			err_msg = cl.Message(content=f'Error processing message: {error_str}\n\nCheck server logs for details.')
		await err_msg.send()