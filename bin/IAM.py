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

def get_existing_role(role_name):
    """Check if IAM role exists and return its ARN"""
    # Initialize IAM client
    session = boto3.Session(
        region_name=os.getenv('AWS_DEFAULT_REGION'),
        profile_name=os.getenv('AWS_PROFILE')
    )
    iam = session.client('iam')
    
    try:
        response = iam.get_role(RoleName=role_name)
        return response['Role']['Arn']
    except Exception:
        return None

def create_dms_role(force_recreate=False):
    """Create IAM role for DMS to access S3"""
    print("Setting up IAM role for DMS to access S3...")
    
    # Check if role exists in run_data
    s3_role_arn = get_resource_data('iam', 's3_role_arn')
    role_name = "dms-s3-access-role"
    
    # If not forcing recreation and role exists in run_data, verify it exists in AWS
    if not force_recreate and s3_role_arn:
        existing_role_arn = get_existing_role(role_name)
        if existing_role_arn:
            print(f"Reusing existing IAM role: {role_name}")
            return existing_role_arn
    
    print("Creating/recreating IAM role for DMS to access S3...")
    
    # Initialize IAM client
    session = boto3.Session(
        region_name=os.getenv('AWS_DEFAULT_REGION'),
        profile_name=os.getenv('AWS_PROFILE')
    )
    iam = session.client('iam')
    
    try:
        # First, check if the role exists and delete it if it does
        try:
            # Check if custom policy exists and detach/delete it
            try:
                # Get all policies attached to the role
                attached_policies = iam.list_attached_role_policies(RoleName=role_name)
                
                # Detach each policy
                for policy in attached_policies.get('AttachedPolicies', []):
                    iam.detach_role_policy(
                        RoleName=role_name,
                        PolicyArn=policy['PolicyArn']
                    )
                    print(f"Detached policy {policy['PolicyName']} from role {role_name}")
                    
                    # If it's our custom policy, delete it
                    if 'dms-s3-access-policy' in policy['PolicyArn']:
                        iam.delete_policy(PolicyArn=policy['PolicyArn'])
                        print(f"Deleted custom policy: {policy['PolicyName']}")
            except Exception as e:
                print(f"Note: Could not detach/delete policies: {e}")
                
            # Delete the role
            iam.delete_role(RoleName=role_name)
            print(f"Deleted existing IAM role: {role_name}")
            # Wait a moment for deletion to propagate
            import time
            time.sleep(5)
        except iam.exceptions.NoSuchEntityException:
            # Role doesn't exist, which is fine
            pass
        except Exception as e:
            print(f"Note: Could not delete existing role: {e}")
        
        # Create the role with the exact trust policy required by DMS
        assume_role_policy_document = json.dumps({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {
                        "Service": "dms.amazonaws.com"
                    },
                    "Action": "sts:AssumeRole"
                }
            ]
        })
        
        # Create the role
        response = iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=assume_role_policy_document,
            Description="Role for AWS DMS to access S3 buckets"
        )
        
        print(f"Created IAM role: {role_name}")
        
        # Create a specific policy for DMS to access S3 based on AWS documentation
        account_id = os.getenv('AWS_ACCOUNT_ID')
        bucket_name = f"dms-target-{account_id}-{os.getenv('AWS_DEFAULT_REGION')}"
        
        # Following AWS's recommended policy for S3 target endpoints
        dms_s3_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "s3:PutObject",
                        "s3:DeleteObject",
                        "s3:PutObjectTagging"
                    ],
                    "Resource": [
                        f"arn:aws:s3:::{bucket_name}/*"
                    ]
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "s3:ListBucket"
                    ],
                    "Resource": [
                        f"arn:aws:s3:::{bucket_name}"
                    ]
                }
            ]
        }
        
        # Create the policy
        policy_name = f"dms-s3-access-policy-{os.getenv('AWS_PROFILE')}"
        try:
            policy_response = iam.create_policy(
                PolicyName=policy_name,
                PolicyDocument=json.dumps(dms_s3_policy),
                Description="Policy for DMS to access S3 bucket"
            )
            policy_arn = policy_response['Policy']['Arn']
            print(f"Created custom S3 policy: {policy_name}")
        except iam.exceptions.EntityAlreadyExistsException:
            # Policy already exists, get its ARN
            policy_arn = f"arn:aws:iam::{account_id}:policy/{policy_name}"
            print(f"Using existing policy: {policy_name}")
        
        # Attach the policy to the role
        iam.attach_role_policy(
            RoleName=role_name,
            PolicyArn=policy_arn
        )
        print(f"Attached custom S3 policy to role {role_name}")
        
        # Wait a moment for the role to propagate
        import time
        time.sleep(15)
        
        return response['Role']['Arn']
    except Exception as e:
        print(f"Error creating IAM role: {e}")
        return None
        
def create_dms_vpc_role(force_recreate=False):
    """Create IAM role for DMS to access VPC resources"""
    print("Setting up IAM role for DMS to access VPC resources...")
    
    # Check if role exists in run_data
    vpc_role_arn = get_resource_data('iam', 'vpc_role_arn')
    role_name = "dms-vpc-role"
    
    # If not forcing recreation and role exists in run_data, verify it exists in AWS
    if not force_recreate and vpc_role_arn:
        existing_role_arn = get_existing_role(role_name)
        if existing_role_arn:
            print(f"Reusing existing IAM role: {role_name}")
            return existing_role_arn
    
    print("Creating/recreating IAM role for DMS to access VPC resources...")
    
    # Initialize IAM client
    session = boto3.Session(
        region_name=os.getenv('AWS_DEFAULT_REGION'),
        profile_name=os.getenv('AWS_PROFILE')
    )
    iam = session.client('iam')
    
    try:
        # First, check if the role exists and delete it if it does
        try:
            # Detach all policies
            attached_policies = iam.list_attached_role_policies(RoleName=role_name)
            for policy in attached_policies.get('AttachedPolicies', []):
                iam.detach_role_policy(
                    RoleName=role_name,
                    PolicyArn=policy['PolicyArn']
                )
                print(f"Detached {policy['PolicyName']} policy from role {role_name}")
            
            # Delete the role
            iam.delete_role(RoleName=role_name)
            print(f"Deleted existing IAM role: {role_name}")
            # Wait a moment for deletion to propagate
            import time
            time.sleep(5)
        except iam.exceptions.NoSuchEntityException:
            # Role doesn't exist, which is fine
            pass
        except Exception as e:
            print(f"Note: Could not delete existing role: {e}")
        
        # Create the role with the exact trust policy required by DMS
        assume_role_policy_document = json.dumps({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {
                        "Service": "dms.amazonaws.com"
                    },
                    "Action": "sts:AssumeRole"
                }
            ]
        })
        
        # Create the role
        response = iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=assume_role_policy_document,
            Description="Role for AWS DMS to access VPC resources"
        )
        
        print(f"Created IAM role: {role_name}")
        
        # Attach the AmazonDMSVPCManagementRole policy
        iam.attach_role_policy(
            RoleName=role_name,
            PolicyArn="arn:aws:iam::aws:policy/service-role/AmazonDMSVPCManagementRole"
        )
        
        print(f"Attached AmazonDMSVPCManagementRole policy to role {role_name}")
        
        # Get the role ARN
        role_arn = response['Role']['Arn']
        
        return role_arn
    except Exception as e:
        print(f"Error creating IAM role for DMS VPC access: {e}")
        return None

def main(force_recreate=False):
    """Main function to create IAM roles for DMS
    
    Args:
        force_recreate (bool): If True, recreate IAM roles even if they exist
    """
    load_config()  # Load config but we don't need the params for this module
    
    # Create S3 access role
    s3_role_arn = create_dms_role(force_recreate=force_recreate)
    
    # Create VPC access role
    vpc_role_arn = create_dms_vpc_role(force_recreate=force_recreate)
    
    if s3_role_arn and vpc_role_arn:
        # Store IAM role ARNs in run_data.json
        update_resource_data('iam', {
            's3_role_arn': s3_role_arn,
            'vpc_role_arn': vpc_role_arn
        })
        
        return s3_role_arn, vpc_role_arn
    else:
        print("IAM role setup failed.")
        return None, None

if __name__ == "__main__":
    main()
