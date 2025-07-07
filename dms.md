# AWS Database Migration Service (DMS) CLI Commands

This document provides the AWS CLI commands needed to set up and run a successful AWS DMS migration task from SQL Server RDS to an S3 bucket.

## Prerequisites

- AWS CLI configured with proper credentials
- Permissions to create and manage AWS DMS resources
- Permissions to create and manage S3 buckets

## Checking DMS Resources

### List Endpoints
```bash
aws dms describe-endpoints
```

### List Replication Instances
```bash
aws dms describe-replication-instances
```

### List Replication Tasks
```bash
aws dms describe-replication-tasks
```

### List Connection Status
```bash
aws dms describe-connections
```

## Testing Connections

### Test Source Endpoint Connection
```bash
aws dms test-connection \
--replication-instance-arn "arn:aws:dms:us-east-1:758045074543:rep:OJSIWQIRVZCC7PUD765KQYT634" \
--endpoint-arn "arn:aws:dms:us-east-1:758045074543:endpoint:NJ3SZNMY3RAWLGFZ5D7ZEHBCXY"
```

### Test Target Endpoint Connection
```bash
aws dms test-connection \
--replication-instance-arn "arn:aws:dms:us-east-1:758045074543:rep:OJSIWQIRVZCC7PUD765KQYT634" \
--endpoint-arn "arn:aws:dms:us-east-1:758045074543:endpoint:QXUC2AQ7A5HDBFWF5Y3DRZYY2I"
```

### Delete Failed Connection Test
```bash
aws dms delete-connection \
--endpoint-arn "arn:aws:dms:us-east-1:758045074543:endpoint:QXUC2AQ7A5HDBFWF5Y3DRZYY2I" \
--replication-instance-arn "arn:aws:dms:us-east-1:758045074543:rep:OJSIWQIRVZCC7PUD765KQYT634"
```

## S3 Bucket Management

### Check if S3 Bucket Exists
```bash
aws s3 ls s3://dms-target-758045074543-us-east-1
```

### Create S3 Bucket
```bash
aws s3 mb s3://dms-target-758045074543-us-east-1 --region us-east-1
```

### Create Folder in S3 Bucket
```bash
touch empty.txt
aws s3 cp empty.txt s3://dms-target-758045074543-us-east-1/dms-data/
rm empty.txt
```

## Endpoint Management

### Modify S3 Target Endpoint
```bash
aws dms modify-endpoint \
--endpoint-arn "arn:aws:dms:us-east-1:758045074543:endpoint:QXUC2AQ7A5HDBFWF5Y3DRZYY2I" \
--s3-settings '{"ServiceAccessRoleArn":"arn:aws:iam::758045074543:role/dms-s3-access-role","BucketName":"dms-target-758045074543-us-east-1","BucketFolder":"dms-data"}'
```

## Table Mappings

### Create Table Mappings File
```bash
cat << 'EOF' > table-mappings.json
{
  "rules": [
    {
      "rule-type": "selection",
      "rule-id": "1",
      "rule-name": "1",
      "object-locator": {
        "schema-name": "dbo",
        "table-name": "raw_src"
      },
      "rule-action": "include"
    }
  ]
}
EOF
```

## Replication Task Management

### Create Replication Task
```bash
aws dms create-replication-task \
--replication-task-identifier "mforbush-sqlserver-to-s3-task" \
--source-endpoint-arn "arn:aws:dms:us-east-1:758045074543:endpoint:NJ3SZNMY3RAWLGFZ5D7ZEHBCXY" \
--target-endpoint-arn "arn:aws:dms:us-east-1:758045074543:endpoint:QXUC2AQ7A5HDBFWF5Y3DRZYY2I" \
--replication-instance-arn "arn:aws:dms:us-east-1:758045074543:rep:OJSIWQIRVZCC7PUD765KQYT634" \
--migration-type full-load \
--table-mappings file://table-mappings.json
```

### Start Replication Task
```bash
aws dms start-replication-task \
--replication-task-arn "arn:aws:dms:us-east-1:758045074543:task:H2KTA62TI5EHTON7RJ7D3UCAFY" \
--start-replication-task-type start-replication
```

### Check Task Status
```bash
aws dms describe-replication-tasks \
--filters Name=replication-task-id,Values=mforbush-sqlserver-to-s3-task
```

### Delete Replication Task
```bash
aws dms delete-replication-task \
--replication-task-arn "arn:aws:dms:us-east-1:758045074543:task:X652YK7KXBFOTERQOBZ2ANNI4M"
```

## Verify Migration Results

### Check S3 for Migrated Data
```bash
aws s3 ls s3://dms-target-758045074543-us-east-1/dms-data/ --recursive
```

## Common Issues and Solutions

### S3 Connection Test Failures

S3 target endpoint connection tests frequently fail despite correct configurations. The most common causes are:

1. **Missing S3 Bucket**: Ensure the bucket exists before testing the connection
   ```bash
   aws s3 mb s3://dms-target-758045074543-us-east-1 --region us-east-1
   ```

2. **Missing Folder Structure**: Create the folder structure specified in the endpoint configuration
   ```bash
   touch empty.txt
   aws s3 cp empty.txt s3://dms-target-758045074543-us-east-1/dms-data/
   rm empty.txt
   ```

3. **Insufficient IAM Permissions**: Verify that the IAM role has proper permissions
   ```bash
   aws iam get-role --role-name dms-s3-access-role
   ```

4. **Simplify S3 Endpoint Settings**: Use minimal settings for S3 endpoints
   ```bash
   aws dms modify-endpoint \
   --endpoint-arn "arn:aws:dms:us-east-1:758045074543:endpoint:QXUC2AQ7A5HDBFWF5Y3DRZYY2I" \
   --s3-settings '{"ServiceAccessRoleArn":"arn:aws:iam::758045074543:role/dms-s3-access-role","BucketName":"dms-target-758045074543-us-east-1","BucketFolder":"dms-data"}'
   ```

### Task Creation/Start Failures

If you encounter issues starting a task, try these steps:

1. Delete and recreate the task
2. Ensure both source and target connection tests are successful
3. Use the `AllowSkipConnectionTest` tag when creating tasks (though this may not always work)
   ```bash
   --tags Key=AllowSkipConnectionTest,Value=true
   ```

## Environment Variables for Scripts

For automating migrations with scripts, consider these environment variables:

- `ACCEPT_S3_CONNECTION_FAILURES`: Set to "true" to automatically continue despite S3 connection test failures
- `SKIP_S3_CONNECTION_TEST`: Set to "true" to skip S3 connection tests entirely
- `AWS_PROFILE`: AWS profile to use
- `AWS_DEFAULT_REGION`: AWS region to use

## Integration with Python Scripts

When integrating with the `DMS.py` script, ensure these key aspects are addressed:

1. **Bucket Existence Check**: Before creating or testing S3 endpoints
   ```python
   def check_s3_bucket_exists(bucket_name):
       s3_client = boto3.client('s3')
       try:
           s3_client.head_bucket(Bucket=bucket_name)
           return True
       except Exception:
           return False
   ```

2. **Minimal S3 Settings**: Use only essential settings for S3 endpoint configuration
   ```python
   s3_settings = {
       'ServiceAccessRoleArn': role_arn,
       'BucketName': bucket_name,
       'BucketFolder': 'dms-data'
   }
   ```

3. **Handling Connection Tests**: Use environment variables to control behavior
   ```python
   if os.getenv('ACCEPT_S3_CONNECTION_FAILURES', '').lower() == 'true':
       print("Continuing despite S3 connection test failure due to ACCEPT_S3_CONNECTION_FAILURES=true")
       return True
   ```