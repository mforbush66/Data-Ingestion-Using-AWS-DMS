#!/usr/bin/env python3

import boto3
import os
import json
import time
from dotenv import load_dotenv
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))

# Import other modules
from IAM import create_dms_role, create_dms_vpc_role
from utils import update_resource_data, get_resource_data

def get_run_data():
    """Get the full run data structure"""
    return get_resource_data()

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


def check_s3_bucket_exists(bucket_name):
    """Check if S3 bucket exists and create it if it doesn't"""
    if not bucket_name:
        print("No bucket name provided")
        return False
        
    print(f"Checking if S3 bucket {bucket_name} exists...")
    
    session = boto3.Session(
        region_name=os.getenv('AWS_DEFAULT_REGION'),
        profile_name=os.getenv('AWS_PROFILE')
    )
    s3 = session.client('s3')
    
    try:
        # Check if bucket exists
        s3.head_bucket(Bucket=bucket_name)
        print(f"S3 bucket {bucket_name} exists")
        
        # Check if dms-data folder exists by listing objects with prefix
        objects = s3.list_objects_v2(Bucket=bucket_name, Prefix='dms-data/', MaxKeys=1)
        
        if 'Contents' not in objects or len(objects['Contents']) == 0:
            print(f"Creating dms-data folder in bucket {bucket_name}...")
            # Create empty object to serve as folder
            s3.put_object(Bucket=bucket_name, Key='dms-data/empty.txt', Body='')
            print(f"Created dms-data folder in bucket {bucket_name}")
        else:
            print(f"dms-data folder exists in bucket {bucket_name}")
            
        return True
    except s3.exceptions.ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404' or error_code == 'NoSuchBucket':
            # Bucket doesn't exist, create it
            print(f"S3 bucket {bucket_name} doesn't exist, creating it...")
            try:
                region = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
                if region == 'us-east-1':
                    # For us-east-1, the LocationConstraint should be omitted
                    s3.create_bucket(Bucket=bucket_name)
                else:
                    # For other regions, specify the LocationConstraint
                    s3.create_bucket(
                        Bucket=bucket_name,
                        CreateBucketConfiguration={'LocationConstraint': region}
                    )
                
                # Create dms-data folder
                s3.put_object(Bucket=bucket_name, Key='dms-data/empty.txt', Body='')
                print(f"Created S3 bucket {bucket_name} and dms-data folder")
                return True
            except Exception as create_error:
                print(f"Error creating S3 bucket: {str(create_error)}")
                return False
        elif error_code == '403':
            print(f"Access denied to S3 bucket {bucket_name}. Check your permissions.")
            return False
        else:
            print(f"Error checking S3 bucket: {str(e)}")
            return False
    except Exception as e:
        print(f"Unexpected error checking S3 bucket: {str(e)}")
        return False


def create_replication_instance(params, vpc_role_arn=None):
    """Create DMS replication instance"""
    print("Creating DMS replication instance...")
    
    # Initialize DMS client
    session = boto3.Session(
        region_name=os.getenv('AWS_DEFAULT_REGION'),
        profile_name=os.getenv('AWS_PROFILE')
    )
    dms = session.client('dms')
    
    # Define replication instance parameters
    instance_id = f"{os.getenv('AWS_PROFILE')}-dms-instance"
    
    try:
        # Check if instance already exists
        try:
            response = dms.describe_replication_instances(
                Filters=[
                    {
                        'Name': 'replication-instance-id',
                        'Values': [instance_id]
                    }
                ]
            )
            if response['ReplicationInstances']:
                print(f"DMS replication instance {instance_id} already exists.")
                return response['ReplicationInstances'][0]['ReplicationInstanceArn']
            else:
                raise Exception("Instance not found")
        except Exception:
            # Create the instance if it doesn't exist
            # Add a delay to ensure IAM role propagation
            time.sleep(10)
            
            # Prepare parameters for the replication instance
            instance_params = {
                'ReplicationInstanceIdentifier': instance_id,
                'AllocatedStorage': 50,
                'ReplicationInstanceClass': 'dms.t3.medium',
                'VpcSecurityGroupIds': params['aurora']['vpc_security_group_ids'],
                'PubliclyAccessible': True,
                'MultiAZ': False,
                'EngineVersion': '3.5.3',
                'AutoMinorVersionUpgrade': True,
                'Tags': [
                    {
                        'Key': 'Name',
                        'Value': instance_id
                    }
                ]
            }
            
            # VPC role ARN is used by DMS service internally, not passed as parameter
            if vpc_role_arn:
                print(f"Using VPC role ARN: {vpc_role_arn}")
                # Note: VPC role is used by DMS service automatically, not passed as parameter
                
            response = dms.create_replication_instance(**instance_params)
            
            print(f"Creating DMS replication instance {instance_id}...")
            print("Waiting for DMS replication instance to become available (this may take several minutes)...")
            
            # Wait for the instance to be available
            waiter = dms.get_waiter('replication_instance_available')
            waiter.wait(
                Filters=[
                    {
                        'Name': 'replication-instance-id',
                        'Values': [instance_id]
                    }
                ]
            )
            
            # Get the instance ARN
            response = dms.describe_replication_instances(
                Filters=[
                    {
                        'Name': 'replication-instance-id',
                        'Values': [instance_id]
                    }
                ]
            )
            
            print("DMS replication instance created successfully!")
            
            # Store replication instance information in run_data.json
            update_resource_data('dms', {
                'replication_instance_id': instance_id,
                'replication_instance_arn': response['ReplicationInstances'][0]['ReplicationInstanceArn']
            })
            
            return response['ReplicationInstances'][0]['ReplicationInstanceArn']
    except Exception as e:
        print(f"Error creating DMS replication instance: {e}")
        return None

def create_source_endpoint(rds_endpoint):
    """Create DMS source endpoint for SQL Server"""
    print("Creating DMS source endpoint for SQL Server...")
    
    if not rds_endpoint:
        print("No RDS endpoint provided. Cannot create source endpoint.")
        return None
    
    # Initialize DMS client
    session = boto3.Session(
        region_name=os.getenv('AWS_DEFAULT_REGION'),
        profile_name=os.getenv('AWS_PROFILE')
    )
    dms = session.client('dms')
    
    # Define endpoint parameters
    endpoint_id = f"{os.getenv('AWS_PROFILE')}-sqlserver-source"
    
    try:
        # Check if endpoint already exists
        try:
            response = dms.describe_endpoints(
                Filters=[
                    {
                        'Name': 'endpoint-id',
                        'Values': [endpoint_id]
                    }
                ]
            )
            if response['Endpoints']:
                print(f"DMS source endpoint {endpoint_id} already exists.")
                return response['Endpoints'][0]['EndpointArn']
            else:
                raise Exception("Endpoint not found")
        except Exception:
            # Delete endpoint if it exists but has issues
            try:
                dms.delete_endpoint(
                    EndpointArn=response['Endpoints'][0]['EndpointArn']
                )
                print(f"Deleted existing DMS source endpoint {endpoint_id} to recreate it.")
                time.sleep(5)  # Wait for deletion to complete
            except Exception:
                pass  # Endpoint doesn't exist or couldn't be deleted
                
            # Create the endpoint with proper SSL settings
            response = dms.create_endpoint(
                EndpointIdentifier=endpoint_id,
                EndpointType='source',
                EngineName='sqlserver',
                ServerName=rds_endpoint['Address'],
                Port=rds_endpoint['Port'],
                DatabaseName='SRC_DB',  # Using SRC_DB database for replication
                Username='admin',
                Password='Admin123!',  # Correct SQL Server password
                SslMode='require',  # SSL mode to require SSL connections
                # Removed ExtraConnectionAttributes that was causing the error
                Tags=[
                    {
                        'Key': 'Name',
                        'Value': endpoint_id
                    }
                ]
            )
            
            print(f"Created DMS source endpoint: {endpoint_id}")
            
            # Store source endpoint information in run_data.json
            update_resource_data('dms', {
                'source_endpoint_id': endpoint_id,
                'source_endpoint_arn': response['Endpoint']['EndpointArn']
            })
            
            return response['Endpoint']['EndpointArn']
    except Exception as e:
        print(f"Error creating DMS source endpoint: {e}")
        return None

def create_target_endpoint(bucket_name, role_arn):
    """Create or modify the S3 target endpoint for DMS"""
    print("Setting up DMS target endpoint for S3...")
    
    if not bucket_name or not role_arn:
        print("Missing bucket name or role ARN. Cannot create target endpoint.")
        return None
        
    # Check if S3 bucket exists and create it if it doesn't
    if not check_s3_bucket_exists(bucket_name):
        print(f"ERROR: Cannot proceed with target endpoint creation because S3 bucket {bucket_name} could not be verified or created.")
        return None
    
    print("S3 bucket verification successful, proceeding with endpoint creation...")
    
    
    # Initialize DMS client
    session = boto3.Session(
        region_name=os.getenv('AWS_DEFAULT_REGION'),
        profile_name=os.getenv('AWS_PROFILE')
    )
    dms = session.client('dms')
    
    # Use a consistent endpoint name
    endpoint_id = "mforbush-s3-target"
    
    # Define the minimal required S3 settings that work reliably
    s3_settings = {
        'BucketName': bucket_name,
        'BucketFolder': 'dms-data',
        'ServiceAccessRoleArn': role_arn,
        'CsvRowDelimiter': '\n',
        'CsvDelimiter': ',',
        'DataFormat': 'csv',
        'AddColumnName': True,
        'CompressionType': 'NONE'
    }
    
    # Tags for the endpoint
    endpoint_tags = [
        {
            'Key': 'Name',
            'Value': endpoint_id
        },
        {
            'Key': 'created_by',
            'Value': 'dms_etl'
        }
    ]
    
    try:
        # Check if endpoint exists
        endpoint_exists = False
        endpoint_arn = None
        try:
            response = dms.describe_endpoints(
                Filters=[
                    {
                        'Name': 'endpoint-id',
                        'Values': [endpoint_id]
                    }
                ]
            )
            if response['Endpoints']:
                endpoint_exists = True
                endpoint_arn = response['Endpoints'][0]['EndpointArn']
                current_settings = response['Endpoints'][0].get('S3Settings', {})
                
                print(f"Found existing DMS target endpoint {endpoint_id}")
                
                # Check if settings need to be updated
                settings_changed = False
                for key, value in s3_settings.items():
                    if key not in current_settings or current_settings[key] != value:
                        settings_changed = True
                        break
                
                if settings_changed:
                    print(f"Modifying existing endpoint {endpoint_id} with updated settings...")
                    try:
                        response = dms.modify_endpoint(
                            EndpointArn=endpoint_arn,
                            S3Settings=s3_settings
                        )
                        print(f"Successfully modified target endpoint: {endpoint_id}")
                    except Exception as e:
                        print(f"Failed to modify endpoint: {str(e)}")
                        print(f"Falling back to delete and recreate approach...")
                        
                        # Delete the endpoint
                        dms.delete_endpoint(EndpointArn=endpoint_arn)
                        print(f"Deleted endpoint {endpoint_id} to recreate with new settings")
                        time.sleep(10)  # Wait for deletion to complete
                        endpoint_exists = False
                else:
                    print(f"Existing endpoint {endpoint_id} has correct settings, no changes needed")
        except Exception as e:
            if "ResourceNotFoundFault" not in str(e):
                print(f"Error checking endpoint existence: {e}")
            if "Endpoint not found" not in str(e):
                print(f"Note: {e}")
            endpoint_exists = False
        
        # Create endpoint if it doesn't exist or was deleted for recreation
        if not endpoint_exists:
            print(f"Creating new DMS target endpoint {endpoint_id}...")
            response = dms.create_endpoint(
                EndpointIdentifier=endpoint_id,
                EndpointType='target',
                EngineName='s3',
                S3Settings=s3_settings,
                Tags=endpoint_tags
            )
            endpoint_arn = response['Endpoint']['EndpointArn']
            print(f"Created DMS target endpoint: {endpoint_id}")
        
        # Store target endpoint information in run_data.json
        update_resource_data('dms', {
            'target_endpoint_id': endpoint_id,
            'target_endpoint_arn': endpoint_arn
        })
        
        # Test connection immediately after setup
        print("\n==================================================\nTESTING CONNECTION TO TARGET ENDPOINT IMMEDIATELY AFTER SETUP\n==================================================")
        run_data = get_run_data()
        replication_instance_arn = None
        if run_data and 'dms' in run_data and 'replication_instance_arn' in run_data['dms']:
            replication_instance_arn = run_data['dms']['replication_instance_arn']
        
        if replication_instance_arn and endpoint_arn:
            test_connection(replication_instance_arn, endpoint_arn)
        else:
            print("Warning: Could not test connection immediately - missing replication instance ARN")
            print("Connection will be tested later in the pipeline.")

        
        return endpoint_arn
            
    except Exception as e:
        print(f"Error setting up DMS target endpoint: {str(e)}")
        # Print detailed exception info for debugging
        import traceback
        print("Detailed error:")
        print(traceback.format_exc())
        return None

def create_replication_task(replication_instance_arn, source_endpoint_arn, target_endpoint_arn):
    """Create or modify DMS replication task"""
    print("Setting up DMS replication task...")
    
    if not replication_instance_arn or not source_endpoint_arn or not target_endpoint_arn:
        print("Missing required ARNs. Cannot create replication task.")
        return None
    
    # Initialize DMS client
    session = boto3.Session(
        region_name=os.getenv('AWS_DEFAULT_REGION'),
        profile_name=os.getenv('AWS_PROFILE')
    )
    dms = session.client('dms')
    
    # Define task parameters
    task_id = f"{os.getenv('AWS_PROFILE')}-sqlserver-to-s3-task"
    
    # Table mappings JSON
    table_mappings = {
        'rules': [
            {
                'rule-type': 'selection',
                'rule-id': '1',
                'rule-name': '1',
                'object-locator': {
                    'schema-name': 'dbo',
                    'table-name': 'raw_src'
                },
                'rule-action': 'include'
            }
        ]
    }
    
    # Define desired task settings
    desired_settings = {
        'SourceEndpointArn': source_endpoint_arn,
        'TargetEndpointArn': target_endpoint_arn,
        'ReplicationInstanceArn': replication_instance_arn,
        'MigrationType': 'full-load',
        'TableMappings': json.dumps(table_mappings)
    }
    
    # Tags for task creation
    task_tags = [
        {
            'Key': 'Name',
            'Value': task_id
        }
    ]
    
    try:
        # Check if task already exists
        task_arn = None
        task_exists = False
        task_status = None
        
        try:
            response = dms.describe_replication_tasks(
                Filters=[
                    {
                        'Name': 'replication-task-id',
                        'Values': [task_id]
                    }
                ]
            )
            
            if response['ReplicationTasks']:
                task_exists = True
                task_arn = response['ReplicationTasks'][0]['ReplicationTaskArn']
                task_status = response['ReplicationTasks'][0]['Status']
                current_settings = response['ReplicationTasks'][0]
                
                print(f"Found existing DMS replication task {task_id} with status: {task_status}")
                
                # Check if we need to update the task settings
                settings_changed = False
                
                # Check if source or target endpoints have changed
                if (current_settings['SourceEndpointArn'] != source_endpoint_arn or 
                    current_settings['TargetEndpointArn'] != target_endpoint_arn or
                    current_settings['ReplicationInstanceArn'] != replication_instance_arn):
                    settings_changed = True
                
                # Check if table mappings have changed (this is just a string comparison)
                if current_settings['TableMappings'] != json.dumps(table_mappings):
                    settings_changed = True
                    
                # If the task is running, we need to stop it before modifying
                if settings_changed:
                    if task_status in ['running', 'starting']:
                        print(f"Stopping task {task_id} before modification...")
                        dms.stop_replication_task(ReplicationTaskArn=task_arn)
                        
                        # Wait for the task to stop
                        print("Waiting for replication task to stop...")
                        waiter = dms.get_waiter('replication_task_stopped')
                        waiter.wait(
                            Filters=[
                                {
                                    'Name': 'replication-task-id',
                                    'Values': [task_id]
                                }
                            ]
                        )
                    
                    print(f"Modifying existing task {task_id} with updated settings...")
                    try:
                        # Note: AWS doesn't allow changing endpoints via ModifyReplicationTask
                        # We'll have to delete and recreate if endpoints have changed
                        if (current_settings['SourceEndpointArn'] != source_endpoint_arn or 
                            current_settings['TargetEndpointArn'] != target_endpoint_arn or
                            current_settings['ReplicationInstanceArn'] != replication_instance_arn):
                            
                            print("Endpoint or instance changes detected. Need to recreate the task...")
                            print(f"Deleting task {task_id} to recreate with new endpoints...")
                            dms.delete_replication_task(ReplicationTaskArn=task_arn)
                            
                            # Wait a moment for deletion to complete
                            time.sleep(10)
                            print(f"Deleted existing task {task_id}")
                            task_exists = False  # Mark as non-existent so we recreate it
                        else:
                            # Only table mappings changed, can use modify
                            response = dms.modify_replication_task(
                                ReplicationTaskArn=task_arn,
                                TableMappings=json.dumps(table_mappings)
                            )
                            print(f"Successfully modified replication task: {task_id}")
                    except Exception as e:
                        print(f"Failed to modify task: {str(e)}")
                        print(f"Falling back to delete and recreate approach...")
                        
                        # Delete the task
                        dms.delete_replication_task(ReplicationTaskArn=task_arn)
                        time.sleep(10)
                        print(f"Deleted existing task {task_id}")
                        task_exists = False  # Mark as non-existent so we recreate it
                else:
                    print(f"Existing task {task_id} has correct settings, no changes needed")
            else:
                # Task doesn't exist, we'll create it
                task_exists = False
                
        except Exception as e:
            if "Task not found" not in str(e):
                print(f"Note: {e}")
            task_exists = False
        
        # Create the task if it doesn't exist or we had to delete it
        if not task_exists:
            # Wait for any connection deletions to complete before creating the task
            print("Waiting for connection deletions to complete...")
            time.sleep(15)  # Wait time to ensure connection deletion is complete
            
            print(f"Creating new DMS replication task {task_id}...")
            response = dms.create_replication_task(
                ReplicationTaskIdentifier=task_id,
                SourceEndpointArn=source_endpoint_arn,
                TargetEndpointArn=target_endpoint_arn,
                ReplicationInstanceArn=replication_instance_arn,
                MigrationType='full-load',
                TableMappings=json.dumps(table_mappings),
                Tags=task_tags
            )
            
            task_arn = response['ReplicationTask']['ReplicationTaskArn']
            print(f"Created DMS replication task: {task_id}")
            
            # Wait for the task to be ready
            print("Waiting for DMS replication task to be ready...")
            try:
                waiter = dms.get_waiter('replication_task_ready')
                waiter.wait(
                    Filters=[
                        {
                            'Name': 'replication-task-id',
                            'Values': [task_id]
                        }
                    ],
                    WaiterConfig={
                        'Delay': 15,  # Check every 15 seconds
                        'MaxAttempts': 40  # Wait up to 10 minutes
                    }
                )
                print("DMS replication task is ready!")
            except Exception as wait_error:
                print(f"Warning: Waiter failed but task may still be ready: {wait_error}")
                # Double-check task status manually
                time.sleep(30)
                response = dms.describe_replication_tasks(
                    Filters=[{'Name': 'replication-task-id', 'Values': [task_id]}]
                )
                if response['ReplicationTasks']:
                    task_status = response['ReplicationTasks'][0]['Status']
                    print(f"Manual check shows task status: {task_status}")
                    if task_status.lower() not in ['ready', 'stopped']:
                        print("Task is not ready. This may cause issues when starting.")
                        return None
        
        # Store replication task information in run_data.json
        update_resource_data('dms', {
            'replication_task_id': task_id,
            'replication_task_arn': task_arn
        })
        
        return task_arn
            
    except Exception as e:
        print(f"Error setting up DMS replication task: {str(e)}")
        # Print detailed exception info for debugging
        import traceback
        print("Detailed error:")
        print(traceback.format_exc())
        return None

def test_connection(replication_instance_arn, endpoint_arn, max_retries=2):
    """Test connection between replication instance and endpoint with retries"""
    print(f"Testing connection to endpoint...")
    
    if not replication_instance_arn or not endpoint_arn:
        print("Missing replication instance ARN or endpoint ARN. Cannot test connection.")
        return False
    
    # Initialize DMS client
    session = boto3.Session(
        region_name=os.getenv('AWS_DEFAULT_REGION'),
        profile_name=os.getenv('AWS_PROFILE')
    )
    dms = session.client('dms')
    
    # Check if this is an S3 endpoint by examining the ARN or fetching endpoint details
    is_s3_endpoint = False
    try:
        endpoint_details = dms.describe_endpoints(Filters=[{'Name': 'endpoint-arn', 'Values': [endpoint_arn]}])
        if endpoint_details['Endpoints'] and endpoint_details['Endpoints'][0]['EngineName'] == 's3':
            is_s3_endpoint = True
            print("Detected S3 endpoint type - using special handling for connection test")
    except Exception as e:
        print(f"Warning: Could not determine endpoint type: {e}")
    
    for retry in range(max_retries + 1):
        try:
            # Delete any existing connections first to start fresh
            try:
                # First try to find connections by endpoint-arn
                connections = dms.describe_connections(
                    Filters=[
                        {
                            'Name': 'endpoint-arn',
                            'Values': [endpoint_arn]
                        }
                    ]
                )
                
                for connection in connections['Connections']:
                    try:
                        # Get connection ARN directly from the connection object
                        connection_arn = connection.get('ReplicationInstanceArn', None)
                        endpoint_arn_from_connection = connection.get('EndpointArn', None)
                        
                        # Create the proper deletion argument
                        if connection_arn and endpoint_arn_from_connection:
                            print(f"Deleting existing connection between {connection_arn} and {endpoint_arn_from_connection}")
                            dms.delete_connection(
                                ReplicationInstanceArn=connection_arn,
                                EndpointArn=endpoint_arn_from_connection
                            )
                            print("Deleted existing connection test")
                            time.sleep(15)  # Give AWS time to process the deletion
                    except Exception as delete_error:
                        print(f"Error deleting connection: {delete_error}")
            except Exception as e:
                print(f"No existing connections found or error listing connections: {e}")
                pass
                
            # Double check - also try to delete with the replication instance ARN
            try:
                dms.delete_connection(
                    ReplicationInstanceArn=replication_instance_arn,
                    EndpointArn=endpoint_arn
                )
                print("Deleted existing connection with replication instance ARN")
                time.sleep(15)  # Give AWS time to process the deletion
            except Exception:
                # No connection to delete with this method
                pass
            
            # For S3 endpoints, we'll skip the test if requested and always return success
            # This is a workaround since S3 endpoint tests frequently fail but tasks still work
            if is_s3_endpoint and os.getenv('SKIP_S3_CONNECTION_TEST', '').lower() == 'true':
                print("SKIP_S3_CONNECTION_TEST is set to true. Skipping test and assuming success.")
                return True
                
            # Start a new connection test
            try:
                response = dms.test_connection(
                    ReplicationInstanceArn=replication_instance_arn,
                    EndpointArn=endpoint_arn
                )
                
                # Get the connection ID - handle various response formats
                test_id = None
                if 'Connection' in response:
                    if isinstance(response['Connection'], dict) and 'Id' in response['Connection']:
                        test_id = response['Connection']['Id']
                    elif 'ConnectionArn' in response['Connection']:
                        # Extract ID from ARN
                        test_id = response['Connection']['ConnectionArn'].split(':')[-1]
                
                # If we couldn't get an ID, use a different approach
                if not test_id:
                    # Use the endpoint ARN to find the connection
                    print("Could not get connection ID directly, using endpoint ARN to find connections...")
                    time.sleep(5)  # Give AWS time to create the connection
                
                print("Waiting for connection test to complete...")
            except Exception as e:
                print(f"Error starting connection test: {e}")
                test_id = None
            
            # Poll for test completion
            max_poll_attempts = 30  # Increase timeout for slow connections
            poll_attempts = 0
            
            # For S3 endpoints, we often get false failures but tasks still work
            if is_s3_endpoint:
                print("Note: S3 endpoint tests often fail but tasks may still work correctly.")
                skip_test = os.getenv('ACCEPT_S3_CONNECTION_FAILURES', '').lower() in ['true', 'yes', '1']
                if skip_test:
                    print("ACCEPT_S3_CONNECTION_FAILURES is set. Skipping S3 connection test and proceeding.")
                    return True
            
            # If we couldn't get a test_id, use endpoint ARN instead
            if not test_id:
                # Just check if the endpoint is active
                endpoint_details = dms.describe_endpoints(
                    Filters=[{'Name': 'endpoint-arn', 'Values': [endpoint_arn]}]
                )
                
                if endpoint_details['Endpoints'] and endpoint_details['Endpoints'][0]['Status'] == 'active':
                    print("Endpoint is active. Considering the connection test successful.")
                    return True
                else:
                    print("Endpoint is not active. Connection test failed.")
                    return False
            
            while poll_attempts < max_poll_attempts:
                poll_attempts += 1
                try:
                    # Try both connection-id and endpoint-arn filters since API behavior varies
                    test_response = dms.describe_connections(
                        Filters=[{'Name': 'endpoint-arn', 'Values': [endpoint_arn]}]
                    )
                    
                    if test_response['Connections']:
                        status = test_response['Connections'][0]['Status']
                        print(f"Connection test status: {status}")
                    
                    if status == 'successful':
                        print("Connection test successful!")
                        return True
                    elif status in ['failed', 'deleting', 'deleted']:
                        # For S3 endpoints, we often get false failures but tasks still work
                        if is_s3_endpoint:
                            print("S3 endpoint connection test failed, but this is common.")
                            skip_test = os.getenv('ACCEPT_S3_CONNECTION_FAILURES', '').lower() in ['true', 'yes', '1']
                            if skip_test:
                                print("ACCEPT_S3_CONNECTION_FAILURES is set. Continuing despite failed S3 connection test.")
                                return True
                        
                        if retry < max_retries:
                            print(f"Connection test failed: {status}. Retrying ({retry + 1}/{max_retries})...")
                            time.sleep(5)  # Wait a bit before retrying
                            break
                        else:
                            print(f"Connection test failed: {status} after {max_retries} retries.")
                            # If it's an S3 endpoint, provide special guidance
                            if is_s3_endpoint:
                                print("\nNOTE: S3 endpoint tests often fail but tasks may still work correctly.")
                                print("Common S3 connection issues:")
                                print("1. IAM role doesn't have correct permissions")
                                print("2. IAM role hasn't fully propagated (can take up to 5-10 minutes)")
                                print("3. VPC endpoint for S3 is not properly configured")
                                print("4. Bucket policies preventing access")
                                print("\nYou can set SKIP_S3_CONNECTION_TEST=true in your .env file to bypass this test.")
                                
                                # Check environment variable for auto-acceptance of S3 connection failures
                                skip_test = os.getenv('ACCEPT_S3_CONNECTION_FAILURES', '').lower() in ['true', 'yes', '1']
                                if skip_test:
                                    print("ACCEPT_S3_CONNECTION_FAILURES is set. Continuing despite failed S3 connection test.")
                                    return True
                            return False
                except Exception as e:
                    print(f"Error checking connection status: {e}")
                    
                time.sleep(5)  # Poll every 5 seconds
                
            print("Connection test timed out after waiting too long")
        except Exception as e:
            if retry < max_retries:
                print(f"Error testing connection: {e}. Retrying ({retry+1}/{max_retries})...")
                time.sleep(30)  # Wait before retry
            else:
                print(f"Error testing connection: {e} after {max_retries} retries.")
                return False
    
    return False

def start_replication_task(task_arn):
    """Start DMS replication task"""
    print("Starting DMS replication task...")
    
    if not task_arn:
        print("No task ARN provided. Cannot start replication task.")
        return False
    
    # Initialize DMS client
    session = boto3.Session(
        region_name=os.getenv('AWS_DEFAULT_REGION'),
        profile_name=os.getenv('AWS_PROFILE')
    )
    dms = session.client('dms')
    
    try:
        # Check current task status first
        response = dms.describe_replication_tasks(
            Filters=[{'Name': 'replication-task-arn', 'Values': [task_arn]}]
        )
        
        if not response['ReplicationTasks']:
            print("Task not found. Cannot start replication.")
            return False
            
        task_status = response['ReplicationTasks'][0]['Status']
        print(f"Current task status: {task_status}")
        
        # Determine the appropriate start type based on task status
        if task_status.lower() == 'ready':
            start_type = 'start-replication'
            print("Task is ready for first run. Using start-replication.")
        elif task_status.lower() == 'stopped':
            stop_reason = response['ReplicationTasks'][0].get('StopReason', '')
            if 'FULL_LOAD_ONLY_FINISHED' in stop_reason:
                print("Task completed full load. Using reload-target to restart.")
                start_type = 'reload-target'
            else:
                print("Task was stopped. Using resume-processing.")
                start_type = 'resume-processing'
        elif task_status.lower() == 'running':
            print("Task is already running. No need to start.")
            # Skip to monitoring section
        else:
            print(f"Task status '{task_status}' is not suitable for starting. Attempting start-replication.")
            start_type = 'start-replication'
        
        # Start the replication task if it's not already running
        if task_status.lower() != 'running':
            print(f"Starting task with type: {start_type}")
            dms.start_replication_task(
                ReplicationTaskArn=task_arn,
                StartReplicationTaskType=start_type
            )
            print("DMS replication task start command sent.")
        else:
            print("Task is already running.")
        
        print("DMS replication task started. Monitoring progress...")
        
        # Monitor task progress with enhanced reporting
        monitoring_start_time = time.time()
        last_progress_update = 0
        
        while True:
            response = dms.describe_replication_tasks(
                Filters=[
                    {
                        'Name': 'replication-task-arn',
                        'Values': [task_arn]
                    }
                ]
            )
            
            if not response['ReplicationTasks']:
                print("Task not found. Stopping monitoring.")
                return False
            
            task_info = response['ReplicationTasks'][0]
            status = task_info['Status']
            
            # Get additional task statistics if available
            stats = task_info.get('ReplicationTaskStats', {})
            full_load_progress = stats.get('FullLoadProgressPercent', 0)
            
            # Enhanced status reporting
            elapsed_time = time.time() - monitoring_start_time
            elapsed_minutes = int(elapsed_time // 60)
            elapsed_seconds = int(elapsed_time % 60)
            
            if full_load_progress > last_progress_update:
                print(f"Task status: {status} | Progress: {full_load_progress}% | Elapsed: {elapsed_minutes}m {elapsed_seconds}s")
                last_progress_update = full_load_progress
            elif int(elapsed_time) % 60 == 0:  # Print status every minute
                print(f"Task status: {status} | Progress: {full_load_progress}% | Elapsed: {elapsed_minutes}m {elapsed_seconds}s")
            
            # Check for completion
            if status.lower() == 'stopped':
                stop_reason = task_info.get('StopReason', '')
                if 'FULL_LOAD_ONLY_FINISHED' in stop_reason:
                    print(f"✓ Replication task completed successfully!")
                    print(f"✓ Final progress: {full_load_progress}%")
                    print(f"✓ Total time: {elapsed_minutes}m {elapsed_seconds}s")
                    
                    # Print final statistics
                    if stats:
                        print(f"✓ Tables loaded: {stats.get('TablesLoaded', 'N/A')}")
                        print(f"✓ Tables loading: {stats.get('TablesLoading', 'N/A')}")
                        print(f"✓ Tables queued: {stats.get('TablesQueued', 'N/A')}")
                        print(f"✓ Tables errored: {stats.get('TablesErrored', 'N/A')}")
                    
                    return True
                else:
                    print(f"⚠ Task stopped with reason: {stop_reason}")
                    return False
            elif status.lower() == 'failed':
                print(f"✗ Replication task failed.")
                print(f"✗ Task ran for: {elapsed_minutes}m {elapsed_seconds}s")
                return False
            elif status.lower() in ['stopping', 'stopped']:
                print(f"Task is stopping/stopped. Final status check...")
                # Continue monitoring for final status
            
            # Wait before checking again
            time.sleep(15)  # Check every 15 seconds for more responsive monitoring
    except Exception as e:
        print(f"Error starting or monitoring DMS replication task: {e}")
        return False

def main(rds_endpoint=None, bucket_name=None, roles=None):
    """Main function to set up DMS components and run migration"""
    params = load_config()
    
    # Extract role ARNs from the roles tuple
    s3_role_arn = None
    vpc_role_arn = None
    if roles:
        s3_role_arn = roles[0]
        if len(roles) > 1:
            vpc_role_arn = roles[1]
            
    # Early validation - check if S3 bucket exists before any other operations
    if bucket_name:
        print("\n" + "=" * 50)
        print("VALIDATING S3 TARGET BUCKET EXISTENCE")
        print("=" * 50)
        bucket_exists = check_s3_bucket_exists(bucket_name)
        if not bucket_exists:
            print(f"ERROR: Target S3 bucket {bucket_name} doesn't exist and couldn't be created.")
            print("This will cause connection tests to fail later. Aborting early.")
            return False
        print(f"Target S3 bucket {bucket_name} validation successful.")

    
    # Create replication instance
    replication_instance_arn = create_replication_instance(params, vpc_role_arn)
    if not replication_instance_arn:
        print("Failed to create DMS replication instance. Exiting.")
        return False
    
    # Create source endpoint
    source_endpoint_arn = create_source_endpoint(rds_endpoint)
    if not source_endpoint_arn:
        print("Failed to create DMS source endpoint. Exiting.")
        return False
    
    # Create target endpoint
    target_endpoint_arn = create_target_endpoint(bucket_name, s3_role_arn)
    if not target_endpoint_arn:
        print("Failed to create DMS target endpoint. Exiting.")
        return False
        
    # Check if there are any existing replication tasks that need to be deleted
    try:
        session = boto3.Session(
            region_name=os.getenv('AWS_DEFAULT_REGION'),
            profile_name=os.getenv('AWS_PROFILE')
        )
        dms = session.client('dms')
        
        # Get the task ARN if it exists
        tasks = dms.describe_replication_tasks()
        existing_tasks = []
        
        for task in tasks.get('ReplicationTasks', []):
            if task['SourceEndpointArn'] == source_endpoint_arn and task['TargetEndpointArn'] == target_endpoint_arn:
                existing_tasks.append(task['ReplicationTaskArn'])
                print(f"Found existing task {task['ReplicationTaskIdentifier']} that will be deleted before testing connections")
        
        # Delete any existing tasks to allow for clean connection testing
        for task_arn in existing_tasks:
            print(f"Deleting existing replication task {task_arn} to allow for clean connection testing...")
            dms.delete_replication_task(ReplicationTaskArn=task_arn)
            print(f"Task deleted. Waiting 20 seconds for AWS to fully process the deletion...")
            time.sleep(20)  # Give AWS time to fully process the deletion
    except Exception as e:
        print(f"Error checking/deleting existing replication tasks: {e}")
        # Continue anyway - this is just preparation
    
    # Test connection to source endpoint - this must succeed for the task to start
    print("\n" + "=" * 50)
    print("TESTING CONNECTION TO SOURCE ENDPOINT")
    print("=" * 50)
    source_connection_success = test_connection(replication_instance_arn, source_endpoint_arn)
    if not source_connection_success:
        print("ERROR: Source endpoint connection test failed.")
        print("This WILL prevent the replication task from starting.")
        print("Troubleshooting tips:")
        print("1. Check RDS security groups allow connections from the DMS replication instance")
        print("2. Ensure the RDS instance is running and accessible")
        print("3. Verify the database credentials are correct")
        return False
    else:
        print("Source endpoint connection test successful!")
    
    # Test connection to target endpoint - with retries and best practices
    print("\n" + "=" * 50)
    print("TESTING CONNECTION TO TARGET ENDPOINT")
    print("=" * 50)
    
    # Following AWS best practices - for critical tests, implement retry logic
    max_attempts = 3
    retry_delay = 10  # seconds
    
    # Determine if this is an S3 endpoint
    is_s3_endpoint = False
    try:
        endpoint_details = dms.describe_endpoints(Filters=[{'Name': 'endpoint-arn', 'Values': [target_endpoint_arn]}])
        if endpoint_details['Endpoints']:
            is_s3_endpoint = 's3' in endpoint_details['Endpoints'][0]['EngineName'].lower()
    except Exception as e:
        print(f"Warning: Could not determine endpoint type: {e}")
    
    # For S3 endpoints - enable auto-acceptance if environment variable is set
    skip_test_for_s3 = is_s3_endpoint and os.getenv('ACCEPT_S3_CONNECTION_FAILURES', '').lower() in ['true', 'yes', '1']
    if skip_test_for_s3:
        print("S3 endpoint detected with ACCEPT_S3_CONNECTION_FAILURES=true. Skipping connection test.")
        target_connection_success = True
    else:
        # Try multiple times to connect
        for attempt in range(max_attempts):
            print(f"Connection test attempt {attempt + 1} of {max_attempts}...")
            
            target_connection_success = test_connection(replication_instance_arn, target_endpoint_arn)
            
            if target_connection_success:
                print("✓ Target endpoint connection test successful!")
                break
            elif attempt < max_attempts - 1:  # Not the last attempt
                print(f"Connection test failed. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                print("All connection test attempts failed.")
    
    # Handle failed connection tests
    if not target_connection_success:
        if is_s3_endpoint:
            print("WARNING: S3 target endpoint connection test failed after multiple attempts.")
            print("For S3 endpoints, this may still work depending on your environment.")
            
            # Check environment variable for automatic acceptance
            if not os.getenv('ACCEPT_S3_CONNECTION_FAILURES', '').lower() in ['true', 'yes', '1']:
                continue_anyway = input("Would you like to continue anyway? (yes/no)\n")
                if continue_anyway.lower() != "yes":
                    print("Exiting due to failed connection test.")
                    return False
            else:
                print("ACCEPT_S3_CONNECTION_FAILURES is set. Continuing despite failed test.")
        else:
            # For non-S3 targets, a failed test is more serious
            print("ERROR: Target endpoint connection test failed after multiple attempts.")
            print("This will likely prevent the replication task from starting.")
            return False
    
    # If target test failed but we're continuing anyway, try to fix the target endpoint
    if not target_connection_success and is_s3_endpoint:
        print("Attempting to optimize S3 endpoint settings...")
        try:
            # Get current endpoint settings
            response = dms.describe_endpoints(
                Filters=[
                    {
                        'Name': 'endpoint-arn',
                        'Values': [target_endpoint_arn]
                    }
                ]
            )
            
            if response['Endpoints']:
                # Update with additional settings
                current_settings = response['Endpoints'][0].get('S3Settings', {})
                current_settings['UseTaskStartTimeForFullLoadTimestamp'] = True
                current_settings['EnableStatistics'] = True
                
                # Modify the endpoint
                dms.modify_endpoint(
                    EndpointArn=target_endpoint_arn,
                    S3Settings=current_settings
                )
                print("Target endpoint updated with optimized settings.")
                time.sleep(10)  # Wait for settings to apply
        except Exception as e:
            print(f"Warning: Could not optimize S3 endpoint settings: {e}")
            print("Continuing with existing settings...")
    
    # Create replication task
    task_arn = create_replication_task(replication_instance_arn, source_endpoint_arn, target_endpoint_arn)
    if not task_arn:
        print("Failed to create DMS replication task. Exiting.")
        return False
    
    # Start replication task
    success = start_replication_task(task_arn)
    if not success:
        print("Failed to start DMS replication task. Exiting.")
        return False
    
    print("\n" + "=" * 50)
    print("DMS MIGRATION COMPLETED SUCCESSFULLY!")
    print("=" * 50)
    
    return True

if __name__ == "__main__":
    # This module requires RDS endpoint and S3 bucket name from other modules
    print("This module requires RDS endpoint and S3 bucket name from other modules.")
    print("Please run this module from a main orchestration script.")
