#!/usr/bin/env python3

import os
import sys
import json
import boto3
from pathlib import Path
import time

# Add parent directory to path for imports
# Import utils module
from utils import update_resource_data, get_resource_data
sys.path.append(str(Path(__file__).parent.parent))

# Import modules
from RDS import main as rds_main
from S3 import main as s3_main
from IAM import main as iam_main
from DMS import main as dms_main

def run_etl_pipeline(force_recreate_roles=False):
    """
    Main orchestration function to run the entire ETL pipeline:
    1. Provision RDS MS SQL Server & initialize schema/data
    2. Create S3 bucket & folder
    3. Create IAM role for DMS
    4. Set up DMS components and run migration
    
    Uses run_data.json to track resources and allow for resuming pipeline execution
    
    Args:
        force_recreate_roles (bool): If True, recreate IAM roles even if they exist
    """
    print("=" * 80)
    print("STARTING AWS DMS ETL PIPELINE")
    print("=" * 80)
    
    # Initialize or update run data with current timestamp
    run_data = get_resource_data()
    update_resource_data('last_run', time.strftime("%Y-%m-%d %H:%M:%S"))
    
    # Step 1: Create new RDS MS SQL Server instance
    print("\n" + "=" * 50)
    print("STEP 1: CREATING NEW RDS MS SQL SERVER")
    print("=" * 50)
    
    # Check if we have an existing RDS instance in run_data.json
    existing_instance = get_resource_data('rds', 'instance_id')
    if existing_instance:
        print(f"Found existing RDS instance in run_data.json: {existing_instance}")
        print("Attempting to use existing instance...")
        # Always initialize the database to ensure SRC_DB and tables exist
        rds_endpoint = rds_main(skip_db_init=False, use_existing=True)
    else:
        # Create a new instance and initialize the database with dynamic naming
        rds_endpoint = rds_main(skip_db_init=False, use_existing=False)
    
    if not rds_endpoint:
        print("Failed to get RDS instance. Exiting pipeline.")
        return False
    
    # Display the endpoint name once it's available
    print(f"\nDatabase endpoint is now available: {rds_endpoint['Address']}:{rds_endpoint['Port']}")
    print(f"Database instance identifier: {get_resource_data('rds', 'instance_id')}")
    print("=" * 50)
    
    # Step 2: Create S3 bucket & folder
    print("\n" + "=" * 50)
    print("STEP 2: CREATING S3 BUCKET & FOLDER")
    print("=" * 50)
    
    # Check if we have an existing S3 bucket in run_data.json
    existing_bucket = get_resource_data('s3', 'bucket_name')
    if existing_bucket:
        print(f"Found existing S3 bucket in run_data.json: {existing_bucket}")
        bucket_name = existing_bucket
    else:
        bucket_name = s3_main()
    
    if not bucket_name:
        print("Failed to create S3 bucket. Exiting pipeline.")
        return False
    
    # Step 3: Create IAM role for DMS
    print("\n" + "=" * 50)
    print("STEP 3: CREATING IAM ROLE FOR DMS")
    print("=" * 50)
    
    # Check if we need to recreate roles or can reuse existing ones
    roles_created = False
    
    # Create or reuse IAM roles
    s3_role_arn, vpc_role_arn = iam_main(force_recreate=force_recreate_roles)
    
    if not s3_role_arn or not vpc_role_arn:
        print("Failed to create IAM roles. Exiting pipeline.")
        return False
    
    # Check if roles were newly created or reused
    existing_s3_role = get_resource_data('iam', 's3_role_arn')
    if force_recreate_roles or not existing_s3_role or s3_role_arn != existing_s3_role:
        roles_created = True
    
    print("IAM role setup completed successfully!")
    print(f"S3 Role ARN: {s3_role_arn}")
    print(f"VPC Role ARN: {vpc_role_arn}")
    
    # Create VPC endpoint for S3 to ensure DMS can access S3 bucket
    print("Creating VPC endpoint for S3...")
    
    # Initialize EC2 client
    if os.getenv('AWS_PROFILE'):
        session = boto3.Session(profile_name=os.getenv('AWS_PROFILE'))
        ec2 = session.client('ec2', region_name=os.getenv('AWS_DEFAULT_REGION'))
    else:
        ec2 = boto3.client('ec2', region_name=os.getenv('AWS_DEFAULT_REGION'))
    
    # Get VPC ID from parameters
    vpc_id = get_resource_data('rds', 'vpc_id')
    endpoint_created = False
    
    if vpc_id:
        try:
            # Check if S3 VPC endpoint already exists
            response = ec2.describe_vpc_endpoints(
                Filters=[
                    {
                        'Name': 'vpc-id',
                        'Values': [vpc_id]
                    },
                    {
                        'Name': 'service-name',
                        'Values': [f'com.amazonaws.{os.getenv("AWS_DEFAULT_REGION")}.s3']
                    }
                ]
            )
            
            if response['VpcEndpoints']:
                endpoint_id = response['VpcEndpoints'][0]['VpcEndpointId']
                print(f"S3 VPC endpoint {endpoint_id} already exists.")
                
                # Store VPC endpoint information in run_data.json
                update_resource_data('vpc', {
                    's3_endpoint_id': endpoint_id
                })
            else:
                # Create S3 VPC endpoint
                response = ec2.create_vpc_endpoint(
                    VpcEndpointType='Gateway',
                    VpcId=vpc_id,
                    ServiceName=f'com.amazonaws.{os.getenv("AWS_DEFAULT_REGION")}.s3',
                    RouteTableIds=[],  # Let AWS automatically associate with all route tables
                    PolicyDocument=json.dumps({
                        "Version": "2012-10-17",
                        "Statement": [
                            {
                                "Effect": "Allow",
                                "Principal": "*",
                                "Action": "s3:*",
                                "Resource": ["arn:aws:s3:::*", "arn:aws:s3:::*/*"]
                            }
                        ]
                    })
                )
                
                endpoint_id = response['VpcEndpoint']['VpcEndpointId']
                print(f"Created S3 VPC endpoint: {endpoint_id}")
                endpoint_created = True
                
                # Store VPC endpoint information in run_data.json
                update_resource_data('vpc', {
                    's3_endpoint_id': endpoint_id
                })
                
                # Wait for the endpoint to be available
                print("Waiting for S3 VPC endpoint to become available...")
                waiter = ec2.get_waiter('vpc_endpoint_exists')
                waiter.wait(
                    VpcEndpointIds=[endpoint_id]
                )
        except Exception as e:
            print(f"Note: Could not create S3 VPC endpoint: {e}")
            print("Continuing without VPC endpoint...")
    
    # Wait for IAM role propagation only if new roles were created or endpoint was created
    if roles_created or endpoint_created:
        wait_time = 180 if roles_created else 60
        print(f"Waiting {wait_time} seconds for IAM roles and VPC endpoints to propagate...")
        print("This delay is necessary for AWS to fully recognize the new resources...")
        time.sleep(wait_time)
    else:
        print("Skipping propagation wait time as existing roles and endpoints are being used.")
        # Brief pause for stability
        time.sleep(5)
    
    # Step 4: Set up DMS components and run migration
    print("\n" + "=" * 50)
    print("STEP 4: SETTING UP DMS COMPONENTS AND RUNNING MIGRATION")
    print("=" * 50)
    
    # Check if we have existing DMS components in run_data.json
    replication_instance_id = get_resource_data('dms', 'replication_instance_id')
    if replication_instance_id:
        print(f"Found existing DMS components in run_data.json")
    
    # Create roles tuple to pass to dms_main
    roles = (s3_role_arn, vpc_role_arn)
    
    success = dms_main(rds_endpoint, bucket_name, roles)
    if not success:
        print("Failed to complete DMS migration. Exiting pipeline.")
        return False
    
    print("\n" + "=" * 80)
    print("ETL PIPELINE COMPLETED SUCCESSFULLY!")
    print("=" * 80)
    print(f"Source: MS SQL Server at {rds_endpoint['Address']}:{rds_endpoint['Port']}")
    print(f"Target: S3 bucket {bucket_name}/dms-data/")
    print("=" * 80)
    
    return True

if __name__ == "__main__":
    import argparse
    
    # Create argument parser
    parser = argparse.ArgumentParser(description='AWS DMS ETL Pipeline Setup')
    parser.add_argument('--force-recreate-roles', action='store_true', 
                        help='Force recreation of IAM roles even if they exist')
    
    # Parse arguments
    args = parser.parse_args()
    
    # Run main function with parsed arguments
    start_time = time.time()
    success = run_etl_pipeline(force_recreate_roles=args.force_recreate_roles)
    end_time = time.time()
    
    print(f"\nTotal execution time: {(end_time - start_time) / 60:.2f} minutes")
    
    if success:
        print("Pipeline executed successfully!")
        sys.exit(0)
    else:
        print("Pipeline execution failed.")
        sys.exit(1)
