#!/usr/bin/env python3
"""
delete_di.py - Destroys everything in AWS related to this project
This script deletes all AWS resources created for the DMS project including:
- DMS replication tasks
- DMS endpoints
- DMS replication instances
- S3 buckets
- RDS instances
- IAM roles and policies
"""

import os
import sys
import boto3
import json
import time
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from pathlib import Path

# Import utils module
sys.path.append(str(Path(__file__).parent))
from utils import get_resource_data

# Load environment variables
load_dotenv('../.env')

# Initialize AWS clients
dms = boto3.client('dms')
s3 = boto3.client('s3')
rds = boto3.client('rds')
iam = boto3.client('iam')
sts = boto3.client('sts')

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
        # Check run_data.json for task information
        task_id = get_resource_data('dms', 'replication_task_id')
        task_arn = get_resource_data('dms', 'replication_task_arn')
        
        # Get all tasks first
        all_tasks = []
        try:
            tasks_response = dms.describe_replication_tasks()
            all_tasks = tasks_response.get('ReplicationTasks', [])
        except ClientError as e:
            print(f"Error listing replication tasks: {e}")
        
        # Add the task from run_data.json if it exists and isn't already in the list
        if task_id and task_arn:
            print(f"Found task in run_data.json: {task_id}")
            if not any(t['ReplicationTaskArn'] == task_arn for t in all_tasks):
                try:
                    task_response = dms.describe_replication_tasks(
                        Filters=[{'Name': 'replication-task-arn', 'Values': [task_arn]}]
                    )
                    if task_response['ReplicationTasks']:
                        all_tasks.append(task_response['ReplicationTasks'][0])
                except ClientError:
                    # Task might not exist anymore
                    pass
        
        # Stop and delete each task
        for task in all_tasks:
            task_arn = task['ReplicationTaskArn']
            task_id = task['ReplicationTaskIdentifier']
            
            print(f"Processing task: {task_id}")
            
            # Stop the task if it's running
            if task['Status'] in ['running', 'starting']:
                print(f"  Stopping task {task_id}...")
                try:
                    dms.stop_replication_task(ReplicationTaskArn=task_arn)
                    
                    # Wait for task to stop
                    max_wait_time = 60  # seconds
                    start_time = time.time()
                    
                    while time.time() - start_time < max_wait_time:
                        try:
                            response = dms.describe_replication_tasks(
                                Filters=[{'Name': 'replication-task-arn', 'Values': [task_arn]}]
                            )
                            if not response['ReplicationTasks']:
                                print(f"  Task {task_id} no longer exists")
                                break
                                
                            status = response['ReplicationTasks'][0]['Status']
                            if status == 'stopped':
                                print(f"  Task {task_id} stopped successfully")
                                break
                                
                            print(f"  Waiting for task to stop... (Status: {status})")
                            time.sleep(5)
                        except ClientError as e:
                            print(f"  Error checking task status: {e}")
                            break
                except ClientError as e:
                    print(f"  Error stopping task: {e}")
            
            # Delete the task
            try:
                dms.delete_replication_task(ReplicationTaskArn=task_arn)
                print(f"  Task {task_id} deletion initiated")
            except ClientError as e:
                if 'already being deleted' in str(e):
                    print(f"  Task {task_id} is already being deleted")
                else:
                    print(f"  Error deleting task {task_id}: {e}")
        
        # Wait for all tasks to be deleted
        print("Waiting for all tasks to be deleted...")
        max_wait_time = 300  # 5 minutes
        start_time = time.time()
        
        while time.time() - start_time < max_wait_time:
            try:
                tasks = dms.describe_replication_tasks()
                if not tasks.get('ReplicationTasks'):
                    print("All replication tasks deleted")
                    return True
                
                # Check if any tasks are still in 'deleting' status
                deleting_tasks = [t for t in tasks['ReplicationTasks']]
                if not deleting_tasks:
                    print("All replication tasks deleted")
                    return True
                    
                print(f"  {len(deleting_tasks)} tasks still exist, waiting...")
                for t in deleting_tasks:
                    print(f"    - {t['ReplicationTaskIdentifier']} (Status: {t['Status']})")
                time.sleep(15)
            except ClientError as e:
                print(f"Error checking tasks: {e}")
                time.sleep(15)
        
        print("WARNING: Timed out waiting for all tasks to be deleted")
        print("Continuing with deletion process, but there may be issues with dependent resources")
        return False
        
    except ClientError as e:
        print(f"Error deleting replication tasks: {e}")

def delete_endpoints():
    """Delete all DMS endpoints"""
    print("Deleting DMS endpoints...")
    
    # First, verify that all replication tasks are deleted
    print("Verifying all replication tasks are deleted before proceeding...")
    try:
        tasks = dms.describe_replication_tasks()
        if tasks.get('ReplicationTasks'):
            print("WARNING: There are still replication tasks that exist!")
            print("Endpoints cannot be deleted while tasks are still present.")
            print("Waiting for tasks to be fully deleted...")
            
            # Wait for up to 3 minutes for tasks to be deleted
            max_wait_time = 180  # 3 minutes
            start_time = time.time()
            
            while time.time() - start_time < max_wait_time:
                tasks = dms.describe_replication_tasks()
                if not tasks.get('ReplicationTasks'):
                    print("All tasks have been deleted, proceeding with endpoint deletion.")
                    break
                    
                print(f"  {len(tasks['ReplicationTasks'])} tasks still exist, waiting...")
                for t in tasks['ReplicationTasks']:
                    print(f"    - {t['ReplicationTaskIdentifier']} (Status: {t['Status']})")
                time.sleep(15)
            else:
                print("WARNING: Timed out waiting for tasks to be deleted.")
                print("Attempting to delete endpoints anyway, but this may fail.")
    except ClientError as e:
        print(f"Error checking replication tasks: {e}")
    
    try:
        # Get all endpoints first
        all_endpoints = []
        try:
            endpoints_response = dms.describe_endpoints()
            all_endpoints = endpoints_response.get('Endpoints', [])
        except ClientError as e:
            print(f"Error listing endpoints: {e}")
        
        # Check run_data.json for endpoint information
        source_endpoint_id = get_resource_data('dms', 'source_endpoint_id')
        source_endpoint_arn = get_resource_data('dms', 'source_endpoint_arn')
        target_endpoint_id = get_resource_data('dms', 'target_endpoint_id')
        target_endpoint_arn = get_resource_data('dms', 'target_endpoint_arn')
        
        # Add endpoints from run_data.json if they exist and aren't already in the list
        if source_endpoint_id and source_endpoint_arn:
            print(f"Found source endpoint in run_data.json: {source_endpoint_id}")
            if not any(e['EndpointArn'] == source_endpoint_arn for e in all_endpoints):
                try:
                    endpoint_response = dms.describe_endpoints(
                        Filters=[{'Name': 'endpoint-arn', 'Values': [source_endpoint_arn]}]
                    )
                    if endpoint_response['Endpoints']:
                        all_endpoints.append(endpoint_response['Endpoints'][0])
                except ClientError:
                    # Endpoint might not exist anymore
                    pass
        
        if target_endpoint_id and target_endpoint_arn:
            print(f"Found target endpoint in run_data.json: {target_endpoint_id}")
            if not any(e['EndpointArn'] == target_endpoint_arn for e in all_endpoints):
                try:
                    endpoint_response = dms.describe_endpoints(
                        Filters=[{'Name': 'endpoint-arn', 'Values': [target_endpoint_arn]}]
                    )
                    if endpoint_response['Endpoints']:
                        all_endpoints.append(endpoint_response['Endpoints'][0])
                except ClientError:
                    # Endpoint might not exist anymore
                    pass
        
        # Delete each endpoint
        for endpoint in all_endpoints:
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
                elif 'InvalidResourceStateFault' in str(e):
                    print(f"  Error: Endpoint {endpoint_id} is still in use by tasks")
                else:
                    print(f"  Error deleting endpoint {endpoint_id}: {e}")
        
        # Wait for all endpoints to be deleted
        print("Waiting for all endpoints to be deleted...")
        max_wait_time = 180  # 3 minutes
        start_time = time.time()
        
        while time.time() - start_time < max_wait_time:
            try:
                endpoints = dms.describe_endpoints()
                if not endpoints.get('Endpoints'):
                    print("All endpoints deleted")
                    return True
                
                print(f"  {len(endpoints['Endpoints'])} endpoints still exist, waiting...")
                time.sleep(10)
            except ClientError as e:
                print(f"Error checking endpoints: {e}")
                time.sleep(10)
        
        print("WARNING: Timed out waiting for all endpoints to be deleted")
        return False
            
        print("All endpoints deleted")
        
    except ClientError as e:
        print(f"Error deleting endpoints: {e}")

def delete_replication_instances():
    """Delete all DMS replication instances"""
    print("Deleting DMS replication instances...")
    
    # First, verify that all endpoints are deleted
    print("Verifying all endpoints are deleted before proceeding...")
    try:
        endpoints = dms.describe_endpoints()
        if endpoints.get('Endpoints'):
            print("WARNING: There are still endpoints that exist!")
            print("Replication instances cannot be deleted while endpoints are still present.")
            print("Waiting for endpoints to be fully deleted...")
            
            # Wait for up to 3 minutes for endpoints to be deleted
            max_wait_time = 180  # 3 minutes
            start_time = time.time()
            
            while time.time() - start_time < max_wait_time:
                endpoints = dms.describe_endpoints()
                if not endpoints.get('Endpoints'):
                    print("All endpoints have been deleted, proceeding with replication instance deletion.")
                    break
                    
                print(f"  {len(endpoints['Endpoints'])} endpoints still exist, waiting...")
                for e in endpoints['Endpoints']:
                    print(f"    - {e['EndpointIdentifier']} (Status: {e.get('Status', 'unknown')})")
                time.sleep(15)
            else:
                print("WARNING: Timed out waiting for endpoints to be deleted.")
                print("Attempting to delete replication instances anyway, but this may fail.")
    except ClientError as e:
        print(f"Error checking endpoints: {e}")
    
    try:
        # Get all replication instances first
        all_instances = []
        try:
            instances_response = dms.describe_replication_instances()
            all_instances = instances_response.get('ReplicationInstances', [])
        except ClientError as e:
            print(f"Error listing replication instances: {e}")
        
        # Check run_data.json for replication instance information
        instance_id = get_resource_data('dms', 'replication_instance_id')
        instance_arn = get_resource_data('dms', 'replication_instance_arn')
        
        # Add instance from run_data.json if it exists and isn't already in the list
        if instance_id and instance_arn:
            print(f"Found replication instance in run_data.json: {instance_id}")
            if not any(i['ReplicationInstanceArn'] == instance_arn for i in all_instances):
                try:
                    instance_response = dms.describe_replication_instances(
                        Filters=[{'Name': 'replication-instance-arn', 'Values': [instance_arn]}]
                    )
                    if instance_response['ReplicationInstances']:
                        all_instances.append(instance_response['ReplicationInstances'][0])
                except ClientError:
                    # Instance might not exist anymore
                    pass
        
        # Delete each instance
        for instance in all_instances:
            instance_arn = instance['ReplicationInstanceArn']
            instance_id = instance['ReplicationInstanceIdentifier']
            
            print(f"Deleting replication instance: {instance_id}")
            
            # Delete the instance
            try:
                dms.delete_replication_instance(ReplicationInstanceArn=instance_arn)
                print(f"  Replication instance {instance_id} deletion initiated")
            except ClientError as e:
                if 'InvalidResourceStateFault' in str(e):
                    print(f"  Error: Instance {instance_id} still has endpoints or tasks")
                else:
                    print(f"  Error deleting replication instance {instance_id}: {e}")
        
        # Wait for all instances to be deleted
        print("Waiting for all replication instances to be deleted...")
        max_wait_time = 300  # 5 minutes
        start_time = time.time()
        
        while time.time() - start_time < max_wait_time:
            try:
                instances = dms.describe_replication_instances()
                if not instances.get('ReplicationInstances'):
                    print("All replication instances deleted")
                    return True
                
                print(f"  {len(instances['ReplicationInstances'])} instances still exist, waiting...")
                time.sleep(30)  # Replication instances take longer to delete
            except ClientError as e:
                print(f"Error checking instances: {e}")
                time.sleep(30)
        
        print("WARNING: Timed out waiting for all replication instances to be deleted")
        print("Continuing with deletion process, but there may be issues with dependent resources")
        return False
        
    except ClientError as e:
        print(f"Error deleting replication instances: {e}")
        return False

def delete_s3_bucket():
    """Delete S3 bucket and all its contents"""
    print("Deleting S3 bucket...")
    
    try:
        # Check run_data.json for bucket information
        bucket_name = get_resource_data('s3', 'bucket_name')
        
        # If not found in run_data.json, use the default naming convention
        if not bucket_name:
            # Use environment variable for region instead of params
            region = os.getenv('AWS_DEFAULT_REGION')
            account_id = sts.get_caller_identity()['Account']
            bucket_name = f"dms-target-{account_id}-{region}"
            print(f"Using default bucket name: {bucket_name}")
        else:
            print(f"Found bucket in run_data.json: {bucket_name}")
        
        # Check if bucket exists
        try:
            s3.head_bucket(Bucket=bucket_name)
        except ClientError as e:
            if '404' in str(e):
                print(f"Bucket {bucket_name} does not exist")
                return
        
        # List all objects in the bucket
        paginator = s3.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name)
        
        # Delete all objects and versions
        for page in pages:
            if 'Contents' in page:
                delete_keys = {'Objects': [{'Key': obj['Key']} for obj in page['Contents']]}
                if delete_keys['Objects']:
                    s3.delete_objects(Bucket=bucket_name, Delete=delete_keys)
                    print(f"Deleted {len(delete_keys['Objects'])} objects from {bucket_name}")
        
        # Delete bucket
        s3.delete_bucket(Bucket=bucket_name)
        print(f"Bucket {bucket_name} deleted")
            
    except ClientError as e:
        print(f"Error deleting S3 bucket: {e}")

def delete_rds_instances():
    """Delete all RDS instances related to this project"""
    print("Deleting RDS instances...")
    
    profile = get_profile()
    
    try:
        # Check run_data.json for RDS instance information
        instance_id = get_resource_data('rds', 'instance_id')
        
        # If found in run_data.json, delete that specific instance
        if instance_id:
            print(f"Found RDS instance in run_data.json: {instance_id}")
            try:
                # Delete the instance
                rds.delete_db_instance(
                    DBInstanceIdentifier=instance_id,
                    SkipFinalSnapshot=True,
                    DeleteAutomatedBackups=True
                )
                print(f"  RDS instance {instance_id} deletion initiated")
            except Exception as e:
                print(f"Error deleting RDS instance from run_data.json: {e}")
        
        # Also list and delete any other instances that might be related to this project
        instances = rds.describe_db_instances()
        
        # Filter instances by profile
        for instance in instances.get('DBInstances', []):
            instance_id = instance['DBInstanceIdentifier']
            
            # Check if this instance belongs to our project
            if profile.lower() in instance_id.lower():
                print(f"Deleting RDS instance: {instance_id}")
                
                # Delete the instance without final snapshot and skip deletion protection
                try:
                    rds.delete_db_instance(
                        DBInstanceIdentifier=instance_id,
                        SkipFinalSnapshot=True,
                        DeleteAutomatedBackups=True
                    )
                    print(f"  RDS instance {instance_id} deletion initiated")
                except ClientError as e:
                    if 'DeletionProtection' in str(e):
                        # Modify instance to disable deletion protection
                        print(f"  Disabling deletion protection for {instance_id}...")
                        rds.modify_db_instance(
                            DBInstanceIdentifier=instance_id,
                            DeletionProtection=False,
                            ApplyImmediately=True
                        )
                        
                        # Wait for modification to complete
                        print(f"  Waiting for modification to complete...")
                        time.sleep(10)
                        
                        # Try deletion again
                        rds.delete_db_instance(
                            DBInstanceIdentifier=instance_id,
                            SkipFinalSnapshot=True,
                            DeleteAutomatedBackups=True
                        )
                        print(f"  RDS instance {instance_id} deletion initiated")
                    else:
                        print(f"  Error deleting RDS instance {instance_id}: {e}")
        
        # Wait for all instances to be deleted
        print("Waiting for RDS instances to be deleted...")
        while True:
            instances = rds.describe_db_instances()
            
            # Filter instances by profile
            project_instances = [
                instance for instance in instances.get('DBInstances', [])
                if profile.lower() in instance['DBInstanceIdentifier'].lower()
            ]
            
            if not project_instances:
                break
                
            print(f"  {len(project_instances)} RDS instances still deleting...")
            time.sleep(30)  # RDS instances take longer to delete
            
        print("All RDS instances deleted")
        
    except ClientError as e:
        print(f"Error deleting RDS instances: {e}")

def delete_iam_roles():
    """Delete IAM roles and policies related to DMS"""
    print("Deleting IAM roles and policies...")
    
    try:
        # Check run_data.json for IAM role information
        s3_role_arn = get_resource_data('iam', 's3_role_arn')
        vpc_role_arn = get_resource_data('iam', 'vpc_role_arn')
        
        # Extract role names from ARNs if available
        dms_s3_role = 'dms-s3-access-role'
        dms_vpc_role = 'dms-vpc-role'
        dms_cloudwatch_role = 'dms-cloudwatch-logs-role'
        
        if s3_role_arn:
            # Extract role name from ARN (format: arn:aws:iam::account-id:role/role-name)
            try:
                dms_s3_role = s3_role_arn.split('/')[-1]
                print(f"Found S3 role in run_data.json: {dms_s3_role}")
            except Exception:
                print(f"Could not parse S3 role ARN: {s3_role_arn}, using default name")
        
        if vpc_role_arn:
            try:
                dms_vpc_role = vpc_role_arn.split('/')[-1]
                print(f"Found VPC role in run_data.json: {dms_vpc_role}")
            except Exception:
                print(f"Could not parse VPC role ARN: {vpc_role_arn}, using default name")
        
        # Delete DMS S3 access role
        try:
            # First detach all policies
            attached_policies = iam.list_attached_role_policies(RoleName=dms_s3_role)
            for policy in attached_policies.get('AttachedPolicies', []):
                policy_arn = policy['PolicyArn']
                print(f"Detaching policy {policy['PolicyName']} from {dms_s3_role}")
                iam.detach_role_policy(RoleName=dms_s3_role, PolicyArn=policy_arn)
            
            # Delete the role
            iam.delete_role(RoleName=dms_s3_role)
            print(f"Role {dms_s3_role} deleted")
        except ClientError as e:
            if 'NoSuchEntity' in str(e):
                print(f"Role {dms_s3_role} does not exist")
            else:
                print(f"Error deleting role {dms_s3_role}: {e}")
        
        # Delete DMS VPC role
        try:
            # First detach all policies
            attached_policies = iam.list_attached_role_policies(RoleName=dms_vpc_role)
            for policy in attached_policies.get('AttachedPolicies', []):
                policy_arn = policy['PolicyArn']
                print(f"Detaching policy {policy['PolicyName']} from {dms_vpc_role}")
                iam.detach_role_policy(RoleName=dms_vpc_role, PolicyArn=policy_arn)
            
            # Delete the role
            iam.delete_role(RoleName=dms_vpc_role)
            print(f"Role {dms_vpc_role} deleted")
        except ClientError as e:
            if 'NoSuchEntity' in str(e):
                print(f"Role {dms_vpc_role} does not exist")
            else:
                print(f"Error deleting role {dms_vpc_role}: {e}")
        
        # Delete DMS CloudWatch logs role
        try:
            # First detach all policies
            attached_policies = iam.list_attached_role_policies(RoleName=dms_cloudwatch_role)
            for policy in attached_policies.get('AttachedPolicies', []):
                policy_arn = policy['PolicyArn']
                print(f"Detaching policy {policy['PolicyName']} from {dms_cloudwatch_role}")
                iam.detach_role_policy(RoleName=dms_cloudwatch_role, PolicyArn=policy_arn)
            
            # Delete the role
            iam.delete_role(RoleName=dms_cloudwatch_role)
            print(f"Role {dms_cloudwatch_role} deleted")
        except ClientError as e:
            if 'NoSuchEntity' in str(e):
                print(f"Role {dms_cloudwatch_role} does not exist")
            else:
                print(f"Error deleting role {dms_cloudwatch_role}: {e}")
            
    except ClientError as e:
        print(f"Error deleting IAM roles: {e}")

def main():
    """Main function"""
    print("Starting deletion process...")
    print("WARNING: This will delete ALL AWS resources related to this project!")
    
    confirmation = input("Are you sure you want to continue? (yes/no): ")
    if confirmation.lower() != 'yes':
        print("Deletion cancelled")
        sys.exit(0)
    
    # Delete in the correct order with proper dependency handling
    print("\n" + "=" * 50)
    print("STEP 1: DELETING DMS REPLICATION TASKS")
    print("=" * 50)
    tasks_deleted = delete_replication_tasks()
    
    print("\n" + "=" * 50)
    print("STEP 2: DELETING DMS ENDPOINTS")
    print("=" * 50)
    # Only proceed with endpoint deletion if tasks were deleted or timed out
    endpoints_deleted = delete_endpoints()
    
    print("\n" + "=" * 50)
    print("STEP 3: DELETING DMS REPLICATION INSTANCES")
    print("=" * 50)
    # Only proceed with instance deletion if endpoints were deleted or timed out
    instances_deleted = delete_replication_instances()
    
    print("\n" + "=" * 50)
    print("STEP 4: DELETING S3 BUCKET")
    print("=" * 50)
    delete_s3_bucket()
    
    print("\n" + "=" * 50)
    print("STEP 5: DELETING RDS INSTANCES")
    print("=" * 50)
    delete_rds_instances()
    
    print("\n" + "=" * 50)
    print("STEP 6: DELETING IAM ROLES")
    print("=" * 50)
    delete_iam_roles()
    
    print("\n" + "=" * 80)
    print("DELETION PROCESS COMPLETED")
    print("=" * 80)
    
    # Check if any resources might still exist
    if not tasks_deleted or not endpoints_deleted or not instances_deleted:
        print("\nWARNING: Some resources might still exist due to timeouts or dependencies.")
        print("You may need to manually check and delete these resources in the AWS console.")
    else:
        print("\nAll AWS resources have been successfully deleted.")
    print("=" * 80)
    
    print("\nDeletion completed. All AWS resources related to this project have been deleted.")

if __name__ == "__main__":
    main()
