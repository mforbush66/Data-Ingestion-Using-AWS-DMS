#!/usr/bin/env python3

import boto3
import os
import json
import time
import pyodbc
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

def create_rds_instance(params):
    """Create MS SQL Server RDS instance"""
    print("Creating RDS MS SQL Server instance...")
    
    # Initialize RDS client
    session = boto3.Session(
        region_name=os.getenv('AWS_DEFAULT_REGION'),
        profile_name=os.getenv('AWS_PROFILE')
    )
    rds = session.client('rds')
    
    # Get current time for unique identifier (hours and minutes only)
    import datetime
    current_time = datetime.datetime.now().strftime("%H-%M")
    
    # Define SQL Server specific parameters
    sql_params = {
        'DBInstanceIdentifier': f"{os.getenv('AWS_PROFILE')}-mssql-source-{current_time}",  # Using timestamp for uniqueness
        'Engine': 'sqlserver-ex',  # SQL Server Express Edition
        'EngineVersion': '15.00.4236.7.v1',  # Version from the AWS console
        'MasterUsername': 'admin',
        'MasterUserPassword': os.getenv('AURORA_DB_PASSWORD'),  # Reusing Aurora password for SQL Server
        'DBInstanceClass': 'db.t3.small',  # Smaller instance class suitable for Express Edition
        'AllocatedStorage': 20,
        'PubliclyAccessible': True,
        'VpcSecurityGroupIds': params['aurora']['vpc_security_group_ids'],
        'LicenseModel': 'license-included',  # Required for SQL Server
        'BackupRetentionPeriod': 0,  # Disable automated backups
        'MultiAZ': False,  # Disable Multi-AZ deployment
        'StorageType': 'gp2',  # General purpose SSD
        'DeletionProtection': False,  # Allow easy deletion
        'CopyTagsToSnapshot': False,  # No need to copy tags to snapshots
        'Tags': [
            {
                'Key': 'Environment',
                'Value': 'Dev/Test'
            }
        ]
    }
    
    try:
        # Check if instance already exists
        try:
            response = rds.describe_db_instances(
                DBInstanceIdentifier=sql_params['DBInstanceIdentifier']
            )
            print(f"RDS instance {sql_params['DBInstanceIdentifier']} already exists.")
            return response['DBInstances'][0]['Endpoint']
        except rds.exceptions.DBInstanceNotFoundFault:
            # Create the instance if it doesn't exist
            response = rds.create_db_instance(**sql_params)
            print(f"Creating RDS instance {sql_params['DBInstanceIdentifier']}...")
            
            # Wait for the instance to be available
            print("Waiting for RDS instance to become available (this may take several minutes)...")
            waiter = rds.get_waiter('db_instance_available')
            waiter.wait(DBInstanceIdentifier=sql_params['DBInstanceIdentifier'])
            
            print(f"RDS instance created successfully!")
            
            # Get the endpoint information
            response = rds.describe_db_instances(DBInstanceIdentifier=sql_params['DBInstanceIdentifier'])
            endpoint = response['DBInstances'][0]['Endpoint']
            
            # Store RDS instance information in run_data.json
            update_resource_data('rds', {
                'instance_id': sql_params['DBInstanceIdentifier'],
                'endpoint': f"{endpoint['Address']}:{endpoint['Port']}"
            })
            
            return endpoint
    except Exception as e:
        print(f"Error creating RDS instance: {e}")
        return None

def initialize_database(endpoint):
    """Initialize MS SQL Server database with schema and sample data"""
    print("Initializing database with schema and sample data...")
    
    if not endpoint:
        print("No endpoint provided. Cannot initialize database.")
        return False
    
    # Wait a bit for the RDS instance to be fully ready for connections
    print("Waiting 30 seconds for RDS instance to be fully available...")
    time.sleep(30)
    
    try:
        # Connect to master database with autocommit for DDL operations
        master_conn_string = f"Driver={{ODBC Driver 17 for SQL Server}};"
        master_conn_string += f"Server={endpoint['Address']},{endpoint['Port']};"
        master_conn_string += f"UID=admin;PWD={os.getenv('AURORA_DB_PASSWORD')};"
        master_conn_string += "Database=master;TrustServerCertificate=yes;Connection Timeout=60;"
        
        print(f"Connecting to SQL Server at {endpoint['Address']}:{endpoint['Port']}...")
        print("Note: If this step fails, it may be due to network/security group restrictions.")
        print("You may need to configure security groups to allow inbound traffic on port 1433.")
        
        # Connect to master database first
        master_conn = pyodbc.connect(master_conn_string)
        master_conn.autocommit = True  # Must be in autocommit mode for CREATE DATABASE
        master_cursor = master_conn.cursor()
        
        # Check if database exists before attempting to create it
        print("Checking if SRC_DB database exists...")
        master_cursor.execute("SELECT name FROM sys.databases WHERE name = 'SRC_DB'")
        db_exists = master_cursor.fetchone()
        
        if not db_exists:
            print("Creating SRC_DB database...")
            master_cursor.execute("CREATE DATABASE SRC_DB;")
        else:
            print("SRC_DB database already exists, skipping creation.")
        
        # Close master connection
        master_cursor.close()
        master_conn.close()
        
        # Connect to the SRC_DB database
        src_conn_string = f"Driver={{ODBC Driver 17 for SQL Server}};"
        src_conn_string += f"Server={endpoint['Address']},{endpoint['Port']};"
        src_conn_string += f"UID=admin;PWD={os.getenv('AURORA_DB_PASSWORD')};"
        src_conn_string += "Database=SRC_DB;TrustServerCertificate=yes;Connection Timeout=60;"
        
        conn = pyodbc.connect(src_conn_string)
        cursor = conn.cursor()
        
        # Explicitly select the database
        print("Selecting SRC_DB database...")
        cursor.execute("USE SRC_DB;")
        conn.commit()
        
        # Check if table exists
        print("Checking if raw_src table exists...")
        cursor.execute("SELECT OBJECT_ID('dbo.raw_src', 'U')")
        table_exists = cursor.fetchone()[0] is not None
        
        if not table_exists:
            # Create table if it doesn't exist
            print("Creating raw_src table...")
            cursor.execute("""
            CREATE TABLE raw_src(
              EMPID INTEGER,
              NAME VARCHAR(50),
              AGE INTEGER,
              GENDER VARCHAR(10),
              LOCATION VARCHAR(50),
              DATE DATE,
              SRC_DTS DATETIME
            );
            """)
            conn.commit()
        else:
            print("raw_src table already exists, skipping creation.")
        
        # Check if data already exists
        cursor.execute("SELECT COUNT(*) FROM raw_src")
        count = cursor.fetchone()[0]
        
        if count == 0:
            # Insert sample data
            print("Inserting sample data...")
            # Get current timestamp for SRC_DTS
            import datetime
            current_timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.000")
            
            rows = [
                (101, 'Robert', 34, 'Male', 'Houston', '2023-04-05', current_timestamp),
                (102, 'Sam', 29, 'Male', 'Dallas', '2023-03-21', current_timestamp),
                (103, 'Smith', 25, 'Male', 'Texas', '2023-04-10', current_timestamp),
                (104, 'Dan', 31, 'Male', 'Florida', '2023-02-07', current_timestamp),
                (105, 'Lily', 27, 'Female', 'Cannes', '2023-01-30', current_timestamp)
            ]
            cursor.executemany(
                "INSERT INTO raw_src VALUES (?,?,?,?,?,?,?);", rows
            )
            conn.commit()
            print(f"Inserted {len(rows)} sample records.")
        else:
            print(f"Table already contains {count} records. Skipping data insertion.")
        
        cursor.close()
        conn.close()
        print("Database initialization completed successfully!")
        return True
    except Exception as e:
        print(f"Error initializing database: {e}")
        return False

def get_existing_rds_endpoint(instance_id):
    """Get the endpoint of an existing RDS instance
    
    Args:
        instance_id (str): The identifier of the existing RDS instance
        
    Returns:
        str: The endpoint of the RDS instance, or None if not found
    """
    try:
        # Create boto3 RDS client
        session = boto3.Session(
            profile_name=os.getenv('AWS_PROFILE'),
            region_name=os.getenv('AWS_REGION')
        )
        rds = session.client('rds')
        
        # Get instance details
        response = rds.describe_db_instances(DBInstanceIdentifier=instance_id)
        
        if response['DBInstances']:
            instance = response['DBInstances'][0]
            if 'Endpoint' in instance and 'Address' in instance['Endpoint']:
                endpoint = {
                    'Address': instance['Endpoint']['Address'],
                    'Port': instance['Endpoint'].get('Port', 1433)  # Default to 1433 if port not specified
                }
                print(f"Found existing RDS instance at endpoint: {endpoint['Address']}:{endpoint['Port']}")
                return endpoint
            else:
                print(f"RDS instance {instance_id} exists but has no endpoint yet (still creating).")
                return None
        else:
            print(f"RDS instance {instance_id} not found.")
            return None
    except Exception as e:
        print(f"Error getting existing RDS endpoint: {e}")
        return None

def main(skip_db_init=False, use_existing=False, existing_instance_id=None):
    """Main function to provision RDS MS SQL Server instance"""
    print("Setting up RDS MS SQL Server instance...")
    params = load_config()
    
    # Check if we should use an existing RDS instance
    if use_existing:
        # Get instance ID from run_data.json
        instance_id = get_resource_data('rds', 'instance_id')
        endpoint_str = get_resource_data('rds', 'endpoint')
        
        if instance_id and endpoint_str:
            print(f"Using existing RDS instance: {instance_id}")
            
            # Initialize RDS client
            session = boto3.Session(
                region_name=os.getenv('AWS_DEFAULT_REGION'),
                profile_name=os.getenv('AWS_PROFILE')
            )
            rds = session.client('rds')
            
            try:
                # Check if the instance exists and is available
                response = rds.describe_db_instances(DBInstanceIdentifier=instance_id)
                status = response['DBInstances'][0]['DBInstanceStatus']
                
                if status == 'available':
                    # Parse endpoint string back into dictionary format
                    host, port = endpoint_str.split(':')
                    endpoint = {'Address': host, 'Port': int(port)}
                    print(f"RDS instance is available at {endpoint_str}")
                    
                    # Initialize database if not skipped
                    if not skip_db_init:
                        initialize_database(endpoint)
                    
                    return endpoint
                else:
                    print(f"RDS instance exists but is in {status} state. Cannot use.")
            except Exception as e:
                print(f"Error checking existing RDS instance: {e}")
                print("Will create a new instance instead.")
    
    # Create new RDS instance if we couldn't use an existing one
    print("Creating new RDS instance...")
    endpoint = create_rds_instance(params)
    
    # Initialize database if not skipped and endpoint was created successfully
    if not skip_db_init and endpoint:
        initialize_database(endpoint)
    
    # Return endpoint for use in other scripts
    return endpoint

if __name__ == "__main__":
    main()
