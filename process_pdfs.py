#!/usr/bin/env python3
"""
Process PDF files from S3 using pypdf, chunk by page, calculate embeddings, 
and save to S3 parquet file.
"""

import os
import sys
import tempfile
from typing import List, Dict, Tuple, Optional
import pandas as pd
import boto3
from botocore.exceptions import ClientError
from pypdf import PdfReader


def get_s3_credentials() -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Get S3 credentials from environment variables.
    Checks S3_* environment variables first, then falls back to AWS_* variables.
    
    Returns:
        Tuple of (access_key_id, secret_access_key, session_token)
    """
    # Try S3-specific credentials first
    access_key_id = os.getenv('S3_ACCESS_KEY_ID') or os.getenv('AWS_ACCESS_KEY_ID')
    secret_access_key = os.getenv('S3_SECRET_ACCESS_KEY') or os.getenv('AWS_SECRET_ACCESS_KEY')
    session_token = os.getenv('S3_SESSION_TOKEN') or os.getenv('AWS_SESSION_TOKEN')
    
    return access_key_id, secret_access_key, session_token


def create_s3_client(
    aws_access_key_id: Optional[str] = None,
    aws_secret_access_key: Optional[str] = None,
    aws_session_token: Optional[str] = None
):
    """
    Create an S3 client with credentials.
    If credentials are not provided, uses get_s3_credentials() to get them from environment.
    
    Args:
        aws_access_key_id: Optional AWS access key ID
        aws_secret_access_key: Optional AWS secret access key
        aws_session_token: Optional AWS session token
    
    Returns:
        boto3 S3 client
    """
    # If credentials not provided, get from environment
    if not aws_access_key_id or not aws_secret_access_key:
        env_access_key, env_secret_key, env_session_token = get_s3_credentials()
        aws_access_key_id = aws_access_key_id or env_access_key
        aws_secret_access_key = aws_secret_access_key or env_secret_key
        aws_session_token = aws_session_token or env_session_token
    
    # Create S3 client with credentials if available
    if aws_access_key_id and aws_secret_access_key:
        client_kwargs = {
            'aws_access_key_id': aws_access_key_id,
            'aws_secret_access_key': aws_secret_access_key
        }
        if aws_session_token:
            client_kwargs['aws_session_token'] = aws_session_token
        return boto3.client('s3', **client_kwargs)
    else:
        # Use default credentials (from environment, IAM role, etc.)
        return boto3.client('s3')


def parse_s3_path(s3_path: str) -> Tuple[str, str]:
    """
    Parse S3 path into bucket and key.
    
    Args:
        s3_path: S3 path in format s3://bucket/key or bucket/key
        
    Returns:
        Tuple of (bucket, key)
    """
    # Remove s3:// prefix if present
    s3_path = s3_path.replace('s3://', '')
    
    # Split into bucket and key
    parts = s3_path.split('/', 1)
    bucket = parts[0]
    key = parts[1] if len(parts) > 1 else ''
    
    return bucket, key


def read_pdfs_from_s3(
    s3_bucket: str, 
    s3_prefix: str = '',
    aws_access_key_id: Optional[str] = None,
    aws_secret_access_key: Optional[str] = None,
    aws_session_token: Optional[str] = None
) -> pd.Series:
    """
    Read PDF files from S3 bucket/key and return a nested pandas Series.
    
    Each element in the Series is a list containing:
    [filename, page_number, page_content]
    
    Args:
        s3_bucket: S3 bucket name
        s3_prefix: S3 prefix (folder path) to search in
        aws_access_key_id: Optional AWS access key ID
        aws_secret_access_key: Optional AWS secret access key
        aws_session_token: Optional AWS session token (for temporary credentials)
        
    Returns:
        pandas.Series where each element is a list [filename, page_number, page_content]
    """
    # Create S3 client with optional credentials
    s3_client = create_s3_client(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        aws_session_token=aws_session_token
    )
    
    # List all PDF files in S3
    pdf_files = list_pdfs_in_s3(
        s3_bucket, 
        s3_prefix,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        aws_session_token=aws_session_token
    )
    
    if not pdf_files:
        print(f"No PDF files found in s3://{s3_bucket}/{s3_prefix}")
        return pd.Series(dtype=object)
    
    print(f"Reading {len(pdf_files)} PDF file(s) from s3://{s3_bucket}/{s3_prefix}...")
    
    all_pages = []
    
    for bucket, key in pdf_files:
        pdf_name = os.path.basename(key)
        print(f"Processing: {pdf_name}")
        
        # Download PDF to temporary file
        with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as tmp_file:
            try:
                s3_client.download_fileobj(bucket, key, tmp_file)
                tmp_file_path = tmp_file.name
            except ClientError as e:
                print(f"Error downloading {key}: {e}")
                continue
        
        try:
            # Load PDF using pypdf
            reader = PdfReader(tmp_file_path)
            
            # Extract pages and create nested structure
            for i, page in enumerate(reader.pages):
                page_content = page.extract_text().strip()
                
                # Skip empty pages
                if not page_content:
                    continue
                
                # Create nested array: [filename, page_number, page_content]
                all_pages.append([pdf_name, i + 1, page_content])
            
            print(f"  Extracted {len(reader.pages)} pages from {pdf_name}")
            
        except Exception as e:
            print(f"Error processing {key}: {e}")
        finally:
            # Clean up temporary file
            try:
                os.unlink(tmp_file_path)
            except:
                pass
    
    # Create pandas Series with nested arrays
    series = pd.Series(all_pages, name='pdf_pages')
    
    print(f"\nTotal pages extracted: {len(series)}")
    return series


def process_pdf_from_s3(
    s3_bucket: str,
    s3_key: str
) -> List[Dict]:
    """
    Process a single PDF file from S3, chunking by page.
    
    Args:
        s3_bucket: S3 bucket name
        s3_key: S3 key (path) to the PDF file
        
    Returns:
        List of dictionaries containing filename, page_num, and page_content
    """
    s3_client = create_s3_client()
    pdf_name = os.path.basename(s3_key)
    
    print(f"Processing PDF: s3://{s3_bucket}/{s3_key}")
    
    # Download PDF to temporary file
    with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as tmp_file:
        try:
            s3_client.download_fileobj(s3_bucket, s3_key, tmp_file)
            tmp_file_path = tmp_file.name
        except ClientError as e:
            print(f"Error downloading {s3_key}: {e}")
            return []
    
    try:
        # Load PDF using pypdf
        reader = PdfReader(tmp_file_path)
        
        results = []
        
        # Process each page as a separate chunk
        for i, page in enumerate(reader.pages):
            page_content = page.extract_text().strip()
            
            # Skip empty pages
            if not page_content:
                continue
            
            # Store results
            results.append({
                'filename': pdf_name,
                'page_num': i + 1,  # 1-indexed page numbers
                'page_content': page_content
            })
        
        print(f"  Processed {len(results)} pages from {pdf_name}")
        return results
        
    except Exception as e:
        print(f"Error processing {s3_key}: {e}")
        return []
    finally:
        # Clean up temporary file
        try:
            os.unlink(tmp_file_path)
        except:
            pass


def list_pdfs_in_s3(
    s3_bucket: str, 
    s3_prefix: str = '',
    aws_access_key_id: Optional[str] = None,
    aws_secret_access_key: Optional[str] = None,
    aws_session_token: Optional[str] = None
) -> List[Tuple[str, str]]:
    """
    List all PDF files in an S3 bucket with the given prefix.
    
    Args:
        s3_bucket: S3 bucket name
        s3_prefix: S3 prefix (folder path) to search in
        aws_access_key_id: Optional AWS access key ID
        aws_secret_access_key: Optional AWS secret access key
        aws_session_token: Optional AWS session token (for temporary credentials)
        
    Returns:
        List of tuples (bucket, key) for each PDF file
    """
    # Create S3 client with optional credentials
    s3_client = create_s3_client(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        aws_session_token=aws_session_token
    )
    pdf_files = []
    
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=s3_bucket, Prefix=s3_prefix)
        
        for page in pages:
            if 'Contents' not in page:
                continue
                
            for obj in page['Contents']:
                key = obj['Key']
                if key.lower().endswith('.pdf'):
                    pdf_files.append((s3_bucket, key))
        
        return pdf_files
        
    except ClientError as e:
        print(f"Error listing S3 objects: {e}")
        return []


def upload_parquet_to_s3(df: pd.DataFrame, s3_bucket: str, s3_key: str):
    """
    Upload a pandas DataFrame as parquet to S3.
    
    Args:
        df: DataFrame to upload
        s3_bucket: S3 bucket name
        s3_key: S3 key (path) for the parquet file
    """
    s3_client = create_s3_client()
    
    # Write to temporary file
    with tempfile.NamedTemporaryFile(delete=False, suffix='.parquet') as tmp_file:
        df.to_parquet(tmp_file.name, index=False, engine='pyarrow')
        tmp_file_path = tmp_file.name
    
    try:
        # Upload to S3
        s3_client.upload_file(tmp_file_path, s3_bucket, s3_key)
        print(f"Successfully uploaded to s3://{s3_bucket}/{s3_key}")
    finally:
        # Clean up temporary file
        try:
            os.unlink(tmp_file_path)
        except:
            pass


def process_pdfs_from_s3(
    input_s3_path: str,
    output_s3_path: str
):
    """
    Process all PDF files from an S3 bucket and save results to S3 parquet file.
    
    Args:
        input_s3_path: S3 path to input bucket/prefix (e.g., s3://my-bucket/pdfs/)
        output_s3_path: S3 path for output parquet file (e.g., s3://my-bucket/output/embeddings.parquet)
    """
    # Parse S3 paths
    input_bucket, input_prefix = parse_s3_path(input_s3_path)
    output_bucket, output_key = parse_s3_path(output_s3_path)
    
    # List all PDF files in S3
    print(f"Listing PDF files in s3://{input_bucket}/{input_prefix}...")
    pdf_files = list_pdfs_in_s3(input_bucket, input_prefix)
    
    if not pdf_files:
        print(f"No PDF files found in s3://{input_bucket}/{input_prefix}")
        return
    
    print(f"Found {len(pdf_files)} PDF file(s) to process\n")
    
    # Process all PDFs sequentially
    print("Processing PDFs...")
    all_results = []
    for i, (bucket, key) in enumerate(pdf_files):
        try:
            results = process_pdf_from_s3(bucket, key)
            all_results.extend(results)
            print(f"Completed {i+1}/{len(pdf_files)} PDFs")
        except Exception as e:
            print(f"Error processing {key}: {e}")
            continue
    
    if not all_results:
        print("No data to save.")
        return
    
    # Create DataFrame
    df = pd.DataFrame(all_results)
    
    # Upload to S3
    print(f"\nSaving {len(df)} chunks to s3://{output_bucket}/{output_key}...")
    upload_parquet_to_s3(df, output_bucket, output_key)
    
    # Print summary
    print(f"\nSummary:")
    print(f"  Total pages: {len(df)}")
    print(f"  Total PDFs processed: {df['filename'].nunique()}")
    
    # Display first row sample
    print_first_row(df)


def print_first_row(df: pd.DataFrame):
    """
    Print the first row of the DataFrame column by column in a human-readable format.
    
    Args:
        df: DataFrame to display
    """
    if len(df) == 0:
        print("\nNo rows to display.")
        return
    
    print("\n" + "="*80)
    print("First Row Sample (Human Readable):")
    print("="*80)
    
    first_row = df.iloc[0]
    
    for column in df.columns:
        print(f"\nColumn: {column}")
        print("-" * 80)
        value = first_row[column]
        
        if column == 'page_content':
            # For page content, show first part and length
            content_text = str(value)
            print(f"  Length: {len(content_text)} characters")
            print(f"  Preview (first 200 chars):")
            print(f"  {content_text[:200]}...")
            if len(content_text) > 200:
                print(f"  ... (truncated, {len(content_text) - 200} more characters)")
        else:
            # For other columns, show the value directly
            print(f"  Value: {value}")
    
    print("\n" + "="*80)


def main():
    """Main entry point."""
    if len(sys.argv) < 3:
        print("Usage: python process_pdfs.py <input_s3_path> <output_s3_path>")
        print("Example: python process_pdfs.py s3://my-bucket/pdfs/ s3://my-bucket/output/embeddings.parquet")
        sys.exit(1)
    
    input_s3_path = sys.argv[1]
    output_s3_path = sys.argv[2]
    
    # Validate S3 paths
    if not (input_s3_path.startswith('s3://') or '/' in input_s3_path):
        print(f"Error: Input path must be an S3 path (s3://bucket/path) or bucket/path format")
        sys.exit(1)
    
    if not (output_s3_path.startswith('s3://') or '/' in output_s3_path):
        print(f"Error: Output path must be an S3 path (s3://bucket/path) or bucket/path format")
        sys.exit(1)
    
    # Check S3 credentials (try S3_* first, then AWS_*)
    access_key, secret_key, session_token = get_s3_credentials()
    
    if not access_key or not secret_key:
        print("Error: S3 credentials not configured.")
        print("Please configure S3 credentials using one of the following:")
        print("  - S3_ACCESS_KEY_ID and S3_SECRET_ACCESS_KEY environment variables (preferred)")
        print("  - AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables (fallback)")
        print("  - AWS credentials file (~/.aws/credentials)")
        print("  - IAM role (if running on EC2)")
        sys.exit(1)
    
    # Check if temporary credentials require session token
    # Temporary AWS credentials (starting with ASIA) require a session token
    if access_key.startswith('ASIA') and not session_token:
        print("Error: Temporary AWS credentials detected (starting with ASIA) but session token is missing.")
        print("Temporary credentials require AWS_SESSION_TOKEN or S3_SESSION_TOKEN to be set.")
        print("Please set one of the following:")
        print("  - S3_SESSION_TOKEN (preferred for S3 operations)")
        print("  - AWS_SESSION_TOKEN (fallback)")
        sys.exit(1)
    
    # Test credentials by creating a client and listing buckets
    try:
        s3_client = create_s3_client()
        s3_client.list_buckets()
    except Exception as e:
        print(f"Error: S3 credentials are invalid or insufficient. {e}")
        print("Please verify your S3 credentials have the necessary permissions.")
        if access_key.startswith('ASIA') and not session_token:
            print("Note: Temporary credentials (starting with ASIA) require a session token.")
        sys.exit(1)
    
    process_pdfs_from_s3(input_s3_path, output_s3_path)


if __name__ == "__main__":
    main()
