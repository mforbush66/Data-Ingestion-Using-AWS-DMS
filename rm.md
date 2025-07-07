````markdown
# Development Roadmap: Python + Boto3 ETL Pipeline

This roadmap implements **all** steps from the PDF documentation via Python/Boto3. It covers:
- RDS MS SQL Server provisioning & schema/data initialization
- S3 bucket & folder creation
- IAM role for DMS
- DMS replication instance, endpoints, and migration task
- (Optional) Redshift provisioning & data loading

---

## 1. Load Config & Environment

1. Install dependencies:
   ```bash
   pip install boto3 python-dotenv pyodbc
````

2. `.env` contains AWS credentials, DB user/pass, region, and endpoints.
3. `config.json` defines resource identifiers (names, sizes, prefixes).
4. In code:

   ```python
   from dotenv import load_dotenv
   import os, json, boto3

   load_dotenv()
   config = json.load(open('config.json'))
   session = boto3.Session(region_name=os.getenv('AWS_REGION'))
   rds = session.client('rds')
   s3  = session.client('s3')
   dms = session.client('dms')
   iam = session.client('iam')
   ```

---

## 2. Provision RDS SQL Server & Initialize Schema

1. **Create MS SQL RDS Instance**

   ```python
   rds.create_db_instance(
       DBInstanceIdentifier=config['rds_identifier'],
       Engine='sqlserver-se',
       MasterUsername=os.getenv('SRC_DB_USER'),
       MasterUserPassword=os.getenv('SRC_DB_PASSWORD'),
       DBInstanceClass=config['rds_class'],
       AllocatedStorage=20,
       PubliclyAccessible=True,
       VpcSecurityGroupIds=config['sg_ids']
   )
   waiter = rds.get_waiter('db_instance_available')
   waiter.wait(DBInstanceIdentifier=config['rds_identifier'])
   endpoint = rds.describe_db_instances()['DBInstances'][0]['Endpoint']
   ```
2. **Connect & Run SQL Commands via pyodbc**

   ```python
   import pyodbc
   conn = pyodbc.connect(
       f"Driver={{ODBC Driver 17 for SQL Server}};"
       f"Server={endpoint['Address']},{endpoint['Port']};"
       f"UID={os.getenv('SRC_DB_USER')};PWD={os.getenv('SRC_DB_PASSWORD')}"
   )
   cur = conn.cursor()

   # Create database
   cur.execute("CREATE DATABASE src_db;")
   # Switch context
   cur.execute("USE src_db;")
   # Create table
   cur.execute("""
   CREATE TABLE raw_src(
     EMPID INTEGER, NAME VARCHAR(50), AGE INTEGER,
     GENDER VARCHAR(10), LOCATION VARCHAR(50),
     DATE DATE, SRC_DTS DATETIME
   );
   """)
   # Insert sample data
   rows = [
     (101,'Robert',34,'Male','Houston','2023-04-05','2023-06-16 00:00:00.000'),
     (102,'Sam',29,'Male','Dallas','2023-03-21','2023-06-16 00:00:00.000'),
     (103,'Smith',25,'Male','Texas','2023-04-10','2023-06-16 00:00:00.000'),
     (104,'Dan',31,'Male','Florida','2023-02-07','2023-06-16 00:00:00.000'),
     (105,'Lily',27,'Female','Cannes','2023-01-30','2023-06-16 00:00:00.000')
   ]
   cur.executemany(
     "INSERT INTO raw_src VALUES (?,?,?,?,?,?,?);", rows
   )
   conn.commit()
   cur.close(); conn.close()
   ```

---

## 3. Create S3 Bucket & Folder

1. **Bucket**

   ```python
   s3.create_bucket(Bucket=config['s3_bucket'])
   ```
2. **Folder (prefix)**

   ```python
   s3.put_object(Bucket=config['s3_bucket'], Key=(config['s3_prefix']+'/'))
   ```

---

## 4. IAM Role for DMS to Access S3

1. **Create role**

   ```python
   assume_doc = json.dumps({
     'Version':'2012-10-17',
     'Statement':[{'Effect':'Allow','Principal':{'Service':'dms.amazonaws.com'},'Action':'sts:AssumeRole'}]
   })
   role = iam.create_role(
     RoleName=config['dms_role'],
     AssumeRolePolicyDocument=assume_doc
   )
   iam.attach_role_policy(
     RoleName=config['dms_role'],
     PolicyArn='arn:aws:iam::aws:policy/AmazonS3FullAccess'
   )
   role_arn = role['Role']['Arn']
   ```

---

## 5. Provision DMS & Endpoints

1. **Replication Instance**

   ```python
   dms.create_replication_instance(
     ReplicationInstanceIdentifier=config['dms_instance'],
     AllocatedStorage=50,
     ReplicationInstanceClass=config['dms_class'],
     PubliclyAccessible=True,
     VpcSecurityGroupIds=config['sg_ids']
   )
   waiter = dms.get_waiter('replication_instance_available')
   waiter.wait(Filters=[{'Name':'replication-instance-id','Values':[config['dms_instance']]}])
   rep_arn = dms.describe_replication_instances()['ReplicationInstances'][0]['ReplicationInstanceArn']
   ```
2. **Source Endpoint**

   ```python
   src_ep = dms.create_endpoint(
     EndpointIdentifier=config['dms_sql_source_id'],
     EndpointType='source', EngineName='sqlserver',
     Username=os.getenv('SRC_DB_USER'), Password=os.getenv('SRC_DB_PASSWORD'),
     ServerName=endpoint['Address'], Port=endpoint['Port'], DatabaseName='src_db'
   )['Endpoint']['EndpointArn']
   ```
3. **Target Endpoint**

   ```python
   tgt_ep = dms.create_endpoint(
     EndpointIdentifier=config['dms_s3_target_id'],EndpointType='target',
     EngineName='s3',
     S3Settings={'BucketName':config['s3_bucket'],'ServiceAccessRoleArn':role_arn}
   )['Endpoint']['EndpointArn']
   ```

---

## 6. Create & Run DMS Migration Task

```python
task = dms.create_replication_task(
  ReplicationTaskIdentifier=config['dms_task'],
  SourceEndpointArn=src_ep,
  TargetEndpointArn=tgt_ep,
  ReplicationInstanceArn=rep_arn,
  MigrationType='full-load',
  TableMappings=json.dumps({
    'rules':[{
      'rule-type':'selection','rule-id':'1','rule-name':'1',
      'object-locator':{'schema-name':'dbo','table-name':'raw_src'},
      'rule-action':'include'
    }]
  })
)['ReplicationTask']['ReplicationTaskArn']

dms.start_replication_task(
  ReplicationTaskArn=task, StartReplicationTaskType='start-replication'
)
# Poll until completion
import time
while True:
  status = dms.describe_replication_tasks(Filters=[{'Name':'replication-task-id','Values':[config['dms_task']]}])
  s = status['ReplicationTasks'][0]['Status']
  if s.lower()=='stopped': break
  time.sleep(30)
```

---

## 7. (Optional) Redshift Provision & Load

*Follows similar Boto3 steps for `redshift.create_cluster` and `psycopg2` COPY commands.*

---

*Run the above scripts in sequence (e.g., in `main.py`) to fully automate the pipeline as per the PDFs.*

```
```
