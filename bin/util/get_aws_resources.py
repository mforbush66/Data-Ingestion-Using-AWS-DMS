#!/usr/bin/env python3
"""
AWS Resource Finder Script

This script helps you find the necessary AWS resources to populate your parameters.json file.
It will look up security groups, subnets, RDS clusters, and other resources in your AWS account.

Prerequisites:
- AWS CLI configured with appropriate credentials
- boto3 library installed (pip install boto3)
- Appropriate IAM permissions to describe resources
"""

import boto3
import json
import os
from botocore.exceptions import ClientError

def get_aws_account_id():
    """Get the current AWS account ID"""
    sts = boto3.client('sts')
    return sts.get_caller_identity()["Account"]

def get_security_groups(vpc_id=None):
    """Get available security groups, optionally filtered by VPC ID"""
    ec2 = boto3.client('ec2')
    filters = [{'Name': 'vpc-id', 'Values': [vpc_id]}] if vpc_id else []
    
    try:
        response = ec2.describe_security_groups(Filters=filters)
        return response['SecurityGroups']
    except ClientError as e:
        print(f"Error getting security groups: {e}")
        return []

def get_subnets(vpc_id=None):
    """Get available subnets, optionally filtered by VPC ID"""
    ec2 = boto3.client('ec2')
    filters = [{'Name': 'vpc-id', 'Values': [vpc_id]}] if vpc_id else []
    
    try:
        response = ec2.describe_subnets(Filters=filters)
        return response['Subnets']
    except ClientError as e:
        print(f"Error getting subnets: {e}")
        return []

def get_rds_clusters():
    """Get available RDS Aurora clusters"""
    rds = boto3.client('rds')
    
    try:
        response = rds.describe_db_clusters()
        return response['DBClusters']
    except ClientError as e:
        print(f"Error getting RDS clusters: {e}")
        return []

def get_s3_buckets():
    """Get available S3 buckets"""
    s3 = boto3.client('s3')
    
    try:
        response = s3.list_buckets()
        return response['Buckets']
    except ClientError as e:
        print(f"Error getting S3 buckets: {e}")
        return []

def get_iam_roles(path_prefix='/service-role/'):
    """Get IAM roles with the specified path prefix"""
    iam = boto3.client('iam')
    
    try:
        response = iam.list_roles(PathPrefix=path_prefix)
        return response['Roles']
    except ClientError as e:
        print(f"Error getting IAM roles: {e}")
        return []

def get_glue_connections():
    """Get existing Glue connections"""
    glue = boto3.client('glue')
    
    try:
        response = glue.get_connections()
        return response.get('ConnectionList', [])
    except ClientError as e:
        print(f"Error getting Glue connections: {e}")
        return []

def load_parameters():
    """Load the current parameters.json file"""
    try:
        with open('parameters.json', 'r') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Error loading parameters.json: {e}")
        return {}

def main():
    """Main function to gather AWS resource information"""
    print("AWS Resource Finder for parameters.json")
    print("=======================================")
    
    # Get AWS account ID
    account_id = get_aws_account_id()
    print(f"\nAWS Account ID: {account_id}")
    
    # Get VPCs
    ec2 = boto3.client('ec2')
    vpcs = ec2.describe_vpcs()['Vpcs']
    print("\nAvailable VPCs:")
    for vpc in vpcs:
        vpc_id = vpc['VpcId']
        name = next((tag['Value'] for tag in vpc.get('Tags', []) if tag['Key'] == 'Name'), 'No Name')
        print(f"  - {vpc_id} ({name})")
    
    # Ask for VPC selection
    selected_vpc = input("\nEnter VPC ID to filter resources (or press Enter for all): ").strip()
    
    # Get security groups
    security_groups = get_security_groups(selected_vpc if selected_vpc else None)
    print("\nAvailable Security Groups:")
    for sg in security_groups:
        print(f"  - {sg['GroupId']} ({sg['GroupName']}): {sg['Description']}")
    
    # Get subnets
    subnets = get_subnets(selected_vpc if selected_vpc else None)
    print("\nAvailable Subnets:")
    for subnet in subnets:
        name = next((tag['Value'] for tag in subnet.get('Tags', []) if tag['Key'] == 'Name'), 'No Name')
        print(f"  - {subnet['SubnetId']} ({name}): {subnet['AvailabilityZone']}, CIDR: {subnet['CidrBlock']}")
    
    # Get RDS clusters
    rds_clusters = get_rds_clusters()
    print("\nAvailable RDS Aurora Clusters:")
    for cluster in rds_clusters:
        print(f"  - {cluster['DBClusterIdentifier']}")
        print(f"    Endpoint: {cluster['Endpoint']}")
        print(f"    Engine: {cluster['Engine']} {cluster['EngineVersion']}")
        print(f"    Database: {cluster.get('DatabaseName', 'N/A')}")
    
    # Get S3 buckets
    s3_buckets = get_s3_buckets()
    print("\nAvailable S3 Buckets:")
    for bucket in s3_buckets:
        print(f"  - {bucket['Name']}")
    
    # Get IAM roles
    glue_roles = get_iam_roles('/service-role/AWSGlue')
    print("\nAvailable Glue Service Roles:")
    for role in glue_roles:
        print(f"  - {role['RoleName']} (ARN: {role['Arn']})")
    
    # Get Glue connections
    glue_connections = get_glue_connections()
    print("\nExisting Glue Connections:")
    for conn in glue_connections:
        print(f"  - {conn['Name']} (Type: {conn['ConnectionType']})")
    
    # Load current parameters
    params = load_parameters()
    
    print("\n\nParameters.json Update Checklist")
    print("==============================")
    print("Copy and paste these values into your parameters.json file:")
    
    print("\n1. Aurora Configuration:")
    print("  - vpc_security_group_ids: Choose from security groups above")
    print("  - db_subnet_group_name: Usually 'default' or create a new one")
    
    print("\n2. Glue Connection:")
    print("  - JDBC_CONNECTION_URL: Use the endpoint from an Aurora cluster above")
    print("  - security_group_id_list: Choose from security groups above")
    print("  - subnet_id: Choose from subnets above")
    
    print("\n3. S3 Locations:")
    print("  - script_location: s3://aws-glue-scripts-{account_id}-{region}/admin/aurora-to-s3-etl.py")
    print("  - TempDir: s3://aws-glue-temporary-{account_id}-{region}/admin")
    print("  - TARGET_S3_LOCATION: Choose an S3 bucket from above")
    print("  - Athena output_location: s3://aws-athena-query-results-{account_id}-{region}/")
    
    print("\n4. IAM Role:")
    print("  - role_name: Choose from Glue service roles above or create a new one")
    
    print("\nRemember to replace the password placeholder with a real password or use AWS Secrets Manager.")

if __name__ == "__main__":
    main()
