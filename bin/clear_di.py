#!/usr/bin/env python3
"""
clear_di.py - Clears DMS components but leaves instances in place
This script deletes replication tasks, endpoints, and clears S3 data,
but leaves RDS and DMS replication instances intact.
"""

import os
import sys
import boto3
import json
import time
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# Load environment variables
load_dotenv('../.env')

# Initialize AWS clients
dms = boto3.client('dms')
s3 = boto3.client('s3')
rds = boto3.client('rds')

def get_profile():
    """Get AWS profile name from environment variables"""
    return os.getenv('AWS_PROFILE', 'default')

def load_parameters():
    """Load parameters from parameters.json"""
    try:
        with open('../parameters.json', 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading parameters: {e}")
        sys.exit(1)

def delete_replication_tasks():
    """Delete all DMS replication tasks"""
    print("Deleting DMS replication tasks...")
    
    try:
        # List all replication tasks
        tasks = dms.describe_replication_tasks()
        
        # Delete each task
        for task in tasks.get('ReplicationTasks', []):
            task_arn = task['ReplicationTaskArn']
            task_id = task['ReplicationTaskIdentifier']
            
            print(f"Deleting task: {task_id}")
            
            # Stop the task if it's running
            if task['Status'] in ['running', 'starting']:
                print(f"  Stopping task {task_id}...")
                dms.stop_replication_task(ReplicationTaskArn=task_arn)
                
                # Wait for task to stop
                while True:
                    response = dms.describe_replication_tasks(
                        Filters=[{'Name': 'replication-task-arn', 'Values': [task_arn]}]
                    )
                    if not response['ReplicationTasks']:
                        break
                    status = response['ReplicationTasks'][0]['Status']
                    if status == 'stopped':
                        break
                    print(f"  Waiting for task to stop... (Status: {status})")
                    time.sleep(5)
            
            # Delete the task
            dms.delete_replication_task(ReplicationTaskArn=task_arn)
            print(f"  Task {task_id} deletion initiated")
        
        # Wait for all tasks to be deleted
        print("Waiting for all tasks to be deleted...")
        while True:
            tasks = dms.describe_replication_tasks()
            if not tasks.get('ReplicationTasks'):
                break
            
            # Check if any tasks are still in 'deleting' status
            deleting_tasks = [t for t in tasks['ReplicationTasks'] if t['Status'] == 'deleting']
            if not deleting_tasks:
                break
                
            print(f"  {len(deleting_tasks)} tasks still deleting...")
            time.sleep(10)
            
        print("All replication tasks deleted")
        
    except ClientError as e:
        print(f"Error deleting replication tasks: {e}")

def delete_endpoints():
    """Delete all DMS endpoints"""
    print("Deleting DMS endpoints...")
    
    try:
        # List all endpoints
        endpoints = dms.describe_endpoints()
        
        # Delete each endpoint
        for endpoint in endpoints.get('Endpoints', []):
            endpoint_arn = endpoint['EndpointArn']
            endpoint_id = endpoint['EndpointIdentifier']
            
            print(f"Deleting endpoint: {endpoint_id}")
            
            # Delete the endpoint
            try:
                dms.delete_endpoint(EndpointArn=endpoint_arn)
                print(f"  Endpoint {endpoint_id} deletion initiated")
            except ClientError as e:
                if 'ResourceNotFoundFault' in str(e):
                    print(f"  Endpoint {endpoint_id} already deleted")
                else:
                    print(f"  Error deleting endpoint {endpoint_id}: {e}")
        
        # Wait for all endpoints to be deleted
        print("Waiting for all endpoints to be deleted...")
        while True:
            endpoints = dms.describe_endpoints()
            if not endpoints.get('Endpoints'):
                break
            
            # Check if any endpoints are still in 'deleting' status
            deleting_endpoints = [e for e in endpoints['Endpoints'] if e['Status'] == 'deleting']
            if not deleting_endpoints:
                break
                
            print(f"  {len(deleting_endpoints)} endpoints still deleting...")
            time.sleep(10)
            
        print("All endpoints deleted")
        
    except ClientError as e:
        print(f"Error deleting endpoints: {e}")

def clear_s3_data():
    """Clear data from S3 bucket but keep the bucket"""
    print("Clearing S3 data...")
    
    try:
        # Use environment variable for region instead of params
        region = os.getenv('AWS_DEFAULT_REGION')
        account_id = boto3.client('sts').get_caller_identity()['Account']
        bucket_name = f"dms-target-{account_id}-{region}"
        
        # List all objects in the bucket
        objects = s3.list_objects_v2(Bucket=bucket_name)
        
        if 'Contents' in objects:
            # Delete all objects
            delete_keys = {'Objects': [{'Key': obj['Key']} for obj in objects['Contents']]}
            if delete_keys['Objects']:
                s3.delete_objects(Bucket=bucket_name, Delete=delete_keys)
                print(f"Deleted {len(delete_keys['Objects'])} objects from {bucket_name}")
            else:
                print(f"No objects to delete in {bucket_name}")
        else:
            print(f"Bucket {bucket_name} is already empty")
            
    except ClientError as e:
        print(f"Error clearing S3 data: {e}")

def clear_database():
    """Clear database tables but keep the database"""
    print("Clearing database tables...")
    
    try:
        # This would require connecting to the database and dropping tables
        # For now, we'll just print a message as this requires direct DB access
        print("Note: Database tables will be recreated when main.py runs with init_db=True")
        
    except Exception as e:
        print(f"Error clearing database: {e}")

def main():
    """Main function"""
    print("Starting cleanup process...")
    
    # Delete replication tasks first
    delete_replication_tasks()
    
    # Delete endpoints
    delete_endpoints()
    
    # Clear S3 data
    clear_s3_data()
    
    # Clear database tables
    clear_database()
    
    print("\nCleanup completed. You can now run main.py to reconstruct the components.")
    print("RDS and DMS replication instances have been preserved.")

if __name__ == "__main__":
    main()
