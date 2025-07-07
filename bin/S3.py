#!/usr/bin/env python3

import boto3
import os
import json
from dotenv import load_dotenv
import sys
from pathlib import Path

# Add parent directory to path for imports
# Import utils module
from utils import update_resource_data, get_resource_data
sys.path.append(str(Path(__file__).parent.parent))

def load_config():
    """Load environment variables and parameters"""
    # Load environment variables from .env file
    env_path = Path(__file__).parent.parent / ".env"
    load_dotenv(dotenv_path=env_path)
    
    # Load parameters from parameters.json
    params_path = Path(__file__).parent.parent / "parameters.json"
    with open(params_path, 'r') as f:
        params = json.load(f)
    
    return params

def create_s3_bucket():
    """Create S3 bucket for DMS target"""
    print("Creating S3 bucket for DMS target...")
    
    # Initialize S3 client
    session = boto3.Session(
        region_name=os.getenv('AWS_DEFAULT_REGION'),
        profile_name=os.getenv('AWS_PROFILE')
    )
    s3 = session.client('s3')
    
    # Generate bucket name using account ID for uniqueness
    account_id = os.getenv('AWS_ACCOUNT_ID')
    bucket_name = f"dms-target-{account_id}-{os.getenv('AWS_DEFAULT_REGION')}"
    
    try:
        # Check if bucket exists
        try:
            s3.head_bucket(Bucket=bucket_name)
            print(f"Bucket {bucket_name} already exists.")
        except s3.exceptions.ClientError as e:
            # If bucket doesn't exist, create it
            if e.response['Error']['Code'] == '404':
                # For regions other than us-east-1, we need to specify LocationConstraint
                if os.getenv('AWS_DEFAULT_REGION') != 'us-east-1':
                    s3.create_bucket(
                        Bucket=bucket_name,
                        CreateBucketConfiguration={
                            'LocationConstraint': os.getenv('AWS_DEFAULT_REGION')
                        }
                    )
                else:
                    # For us-east-1, we don't specify LocationConstraint
                    s3.create_bucket(Bucket=bucket_name)
                    
                print(f"Created bucket: {bucket_name}")
                # Store bucket information in run_data.json
                update_resource_data('s3', {
                    'bucket_name': bucket_name
                })
            else:
                # Some other error occurred
                raise e
        
        return bucket_name
    except Exception as e:
        print(f"Error creating S3 bucket: {e}")
        return None

def create_s3_folder(bucket_name, folder_name="dms-data"):
    """Create folder (prefix) in S3 bucket"""
    print(f"Creating folder {folder_name}/ in bucket {bucket_name}...")
    
    if not bucket_name:
        print("No bucket name provided. Cannot create folder.")
        return False
    
    try:
        # Initialize S3 client
        session = boto3.Session(
            region_name=os.getenv('AWS_DEFAULT_REGION'),
            profile_name=os.getenv('AWS_PROFILE')
        )
        s3 = session.client('s3')
        
        # Create folder (empty object with trailing slash)
        s3.put_object(Bucket=bucket_name, Key=f"{folder_name}/")
        print(f"Created folder: {folder_name}/ in bucket {bucket_name}")
        
        # Store folder information in run_data.json
        update_resource_data('s3', {
            'folder': folder_name
        })
        
        return True
    except Exception as e:
        print(f"Error creating S3 folder: {e}")
        return False

def main():
    """Main function to create S3 bucket and folder"""
    load_config()  # Load config but we don't need the params for this module
    bucket_name = create_s3_bucket()
    if bucket_name:
        success = create_s3_folder(bucket_name)
        if success:
            print("S3 setup completed successfully!")
            return bucket_name
        else:
            print("S3 setup failed during folder creation.")
            return bucket_name  # Still return bucket name as it was created
    else:
        print("S3 setup failed during bucket creation.")
        return None

if __name__ == "__main__":
    main()
