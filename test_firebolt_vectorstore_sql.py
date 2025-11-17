#!/usr/bin/env python3
"""
Test script for Firebolt Vector Store similarity search.
Tests actual similarity search using a live Firebolt connection.
"""

import os
import sys
from langchain_community.vectorstores.firebolt import Firebolt, FireboltSettings

def main():
    """Test Firebolt vector store similarity search with live connection."""
    
    print("=" * 80)
    print("Firebolt Vector Store Similarity Search Test")
    print("=" * 80)
    print()
    
    # Load configuration from environment variables
    try:
        # Determine API endpoint - use staging endpoint if MCP API URL indicates staging
        mcp_api_url = os.getenv("FIREBOLT_MCP_API_URL", "")
        api_endpoint = None
        if mcp_api_url:
            # Check if this is a staging environment
            if "staging" in mcp_api_url.lower():
                # Use the staging API endpoint format
                api_endpoint = "https://api.staging.firebolt.io"
            else:
                # Extract from MCP API URL for other environments
                base_url = mcp_api_url.split("?")[0]
                if not base_url.startswith("http"):
                    api_endpoint = f"https://{base_url}"
                else:
                    api_endpoint = base_url
        
        config = FireboltSettings(
            firebolt_id=os.getenv("FIREBOLT_ID"),
            firebolt_secret=os.getenv("FIREBOLT_SECRET"),
            engine_name=os.getenv("FIREBOLT_ENGINE_NAME", "ingestion_engine"),
            database=os.getenv("FIREBOLT_DATABASE", "data_chat_demo"),
            account_name=os.getenv("FIREBOLT_ACCOUNT_NAME", "developer"),
            semantic_index=os.getenv("FIREBOLT_SEMANTIC_INDEX", "pdf_semantic_index"),
            llm_location=os.getenv("FIREBOLT_LLM_LOCATION", "llm_api"),
            embedding_model="amazon.titan-embed-text-v2:0",
            embedding_dimensions=256,
            api_endpoint=api_endpoint
        )
        
        print(f"Configuration:")
        print(f"  Database: {config.database}")
        print(f"  Semantic Index: {config.semantic_index}")
        print(f"  Engine: {config.engine_name}")
        print(f"  Account: {config.account_name}")
        if config.api_endpoint:
            print(f"  API Endpoint: {config.api_endpoint}")
        print()
        
    except Exception as e:
        print(f"❌ Error loading configuration: {e}")
        print("\nMake sure you have set the following environment variables:")
        print("  - FIREBOLT_ID")
        print("  - FIREBOLT_SECRET")
        print("  - FIREBOLT_ENGINE_NAME")
        print("  - FIREBOLT_DATABASE")
        print("  - FIREBOLT_ACCOUNT_NAME")
        print("  - FIREBOLT_SEMANTIC_INDEX")
        print("  - FIREBOLT_LLM_LOCATION")
        sys.exit(1)
    
    try:
        # Initialize vector store
        print("Initializing Firebolt vector store...")
        vector_store = Firebolt(config=config)
        print("✓ Vector store initialized successfully")
        print()
        
        # Test 1: Basic similarity search
        print("Test 1: Basic Similarity Search")
        print("-" * 80)
        query = "What are the requirements for air traffic controllers?"
        print(f"Query: {query}")
        print()
        
        results = vector_store.similarity_search(query, k=20)
        
        print(f"✓ Found {len(results)} results:")
        print()
        for i, doc in enumerate(results, 1):
            print(f"Result {i}:")
            print(f"  Content: {doc.page_content[:300]}...")
            if hasattr(doc, 'metadata') and doc.metadata:
                print(f"  Metadata: {doc.metadata}")
            print()
        
        # Test 2: Similarity search with score
        print("Test 2: Similarity Search with Score")
        print("-" * 80)
        query2 = "FAA regulations"
        print(f"Query: {query2}")
        print()
        
        results_with_score = vector_store.similarity_search_with_score(query2, k=20)
        
        print(f"✓ Found {len(results_with_score)} results:")
        print()
        for i, (doc, score) in enumerate(results_with_score, 1):
            print(f"Result {i} (similarity score: {score:.6f}):")
            print(f"  Content: {doc.page_content[:300]}...")
            print()
        
        # Test 3: Test embedding generation
        print("Test 3: Embedding Generation Test")
        print("-" * 80)
        test_text = "This is a test query for embedding generation"
        print(f"Test text: {test_text}")
        
        embedding = vector_store._get_embedding(test_text)
        print(f"✓ Embedding generated successfully")
        print(f"  Embedding dimension: {len(embedding)}")
        print(f"  First 5 values: {embedding[:5]}")
        print()
        
        print("=" * 80)
        print("✓ All tests passed!")
        print("=" * 80)
        
    except Exception as e:
        error_str = str(e)
        print(f"\n❌ Error during testing: {e}")
        
        # Provide helpful guidance for common errors
        if "AuthenticationError" in error_str or "Failed to authenticate" in error_str:
            print("\n" + "=" * 80)
            print("AUTHENTICATION ERROR")
            print("=" * 80)
            print("\nThe Firebolt SDK is unable to authenticate with the provided credentials.")
            print("\nPossible causes:")
            print("1. Client ID and Secret don't match the Firebolt environment/domain")
            print("2. The API endpoint URL format may be incorrect")
            print("3. The credentials may be for a different Firebolt account/environment")
            print("\nTo fix:")
            print("- Verify your FIREBOLT_ID and FIREBOLT_SECRET are correct")
            print("- Check if you need different credentials for the staging environment")
            print("- Contact Firebolt support if credentials are correct but still failing")
        elif "does not exist" in error_str and "Engine" in error_str:
            print("\n" + "=" * 80)
            print("ENGINE NOT FOUND")
            print("=" * 80)
            print("\nThe specified engine does not exist or you don't have permission to use it.")
            print("\nTo fix:")
            print(f"- Verify the engine '{config.engine_name}' exists in your Firebolt account")
            print("- Check that you have permission to use this engine")
            print("- Update FIREBOLT_ENGINE_NAME in envvars.sh to use a valid user engine")
            print("\nNote: You need a USER engine (not 'system') to use AI_EMBED_TEXT()")
        elif "doesn't support AI_EMBED_TEXT" in error_str:
            print("\n" + "=" * 80)
            print("ENGINE TYPE ERROR")
            print("=" * 80)
            print("\nThe system engine doesn't support AI_EMBED_TEXT().")
            print("\nTo fix:")
            print("- Use a USER engine instead of the 'system' engine")
            print("- Update FIREBOLT_ENGINE_NAME in envvars.sh to use a valid user engine")
        elif "does not exist" in error_str and "semantic_index" in error_str.lower():
            print("\n" + "=" * 80)
            print("SEMANTIC INDEX NOT FOUND")
            print("=" * 80)
            print(f"\nThe semantic index '{config.semantic_index}' does not exist.")
            print("\nTo fix:")
            print(f"- Create the semantic index '{config.semantic_index}' in your database")
            print("- Ensure the index is populated with data")
            print("- Update FIREBOLT_SEMANTIC_INDEX in envvars.sh if using a different index name")
        else:
            import traceback
            traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()

