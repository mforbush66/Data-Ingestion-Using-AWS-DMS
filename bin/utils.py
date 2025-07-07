#!/usr/bin/env python3

import json
import os
import datetime
from pathlib import Path

def load_run_data():
    """Load the run data from the run_data.json file"""
    run_data_path = Path(__file__).parent.parent / "run_data.json"
    try:
        with open(run_data_path, 'r') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        # Return default structure if file doesn't exist or is invalid
        return {
            "last_run": None,
            "resources": {
                "rds": {
                    "instance_id": None,
                    "endpoint": None
                },
                "s3": {
                    "bucket_name": None,
                    "folder": None
                },
                "iam": {
                    "s3_role_arn": None,
                    "vpc_role_arn": None
                },
                "dms": {
                    "replication_instance_id": None,
                    "source_endpoint_id": None,
                    "target_endpoint_id": None,
                    "replication_task_id": None
                }
            }
        }

def save_run_data(run_data):
    """Save the run data to the run_data.json file"""
    run_data_path = Path(__file__).parent.parent / "run_data.json"
    # Update last run timestamp
    run_data["last_run"] = datetime.datetime.now().isoformat()
    with open(run_data_path, 'w') as f:
        json.dump(run_data, f, indent=2)

def update_resource_data(resource_type, resource_data):
    """Update specific resource data in the run_data.json file
    
    Args:
        resource_type (str): Type of resource (rds, s3, iam, dms) or top-level key (last_run)
        resource_data (dict or str): Dictionary of resource data to update or string value for top-level keys
    """
    run_data = load_run_data()
    
    # Handle top-level keys like 'last_run'
    if resource_type == 'last_run':
        run_data["last_run"] = resource_data
    else:
        # Ensure the resource type exists
        if resource_type not in run_data["resources"]:
            run_data["resources"][resource_type] = {}
        
        # Update the resource data
        run_data["resources"][resource_type].update(resource_data)
    
    # Save the updated data
    save_run_data(run_data)
    
    return run_data

def get_resource_data(resource_type=None, resource_key=None):
    """Get resource data from the run_data.json file
    
    Args:
        resource_type (str, optional): Type of resource (rds, s3, iam, dms)
        resource_key (str, optional): Specific resource key to retrieve
        
    Returns:
        The requested resource data or None if not found
    """
    run_data = load_run_data()
    
    if not resource_type:
        return run_data
    
    if resource_type not in run_data["resources"]:
        return None
    
    if not resource_key:
        return run_data["resources"][resource_type]
    
    return run_data["resources"][resource_type].get(resource_key)
