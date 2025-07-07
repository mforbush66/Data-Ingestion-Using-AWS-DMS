# AWS DMS Data Ingestion Pipeline

A comprehensive ETL pipeline using AWS Database Migration Service (DMS) to migrate data from SQL Server to S3. This project demonstrates end-to-end data ingestion using AWS services including RDS, DMS, S3, and IAM.

## 🏗️ Architecture Overview

This pipeline orchestrates the following AWS services:

1. **RDS MS SQL Server** - Source database with sample data
2. **AWS DMS** - Database Migration Service for data replication
3. **S3** - Target storage for migrated data
4. **IAM** - Identity and Access Management for service permissions

## 📋 Prerequisites

- Python 3.7+
- AWS CLI configured with appropriate permissions
- AWS account with DMS service access
- Required Python packages (see Installation section)

## 🚀 Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd "Data Ingestion using AWS DMS"
   ```

2. **Install Python dependencies**
   ```bash
   pip install boto3 python-dotenv pyodbc
   ```

3. **Configure AWS credentials**
   - Set up AWS CLI: `aws configure`
   - Or use IAM roles/instance profiles

4. **Set up configuration files**
   ```bash
   # Copy example files and customize
   cp bin/.env.example bin/.env
   cp bin/parameters.json.example bin/parameters.json
   ```

## ⚙️ Configuration

### Environment Variables (bin/.env)

```bash
# AWS Credentials
AWS_ACCESS_KEY_ID=your_access_key_here
AWS_SECRET_ACCESS_KEY=your_secret_key_here
AWS_DEFAULT_REGION=us-east-1
AWS_PROFILE=your-aws-profile

# Database Credentials
AURORA_DB_PASSWORD=your_secure_password_here

# AWS Account ID
AWS_ACCOUNT_ID=your_account_id_here
```

### Parameters (bin/parameters.json)

Key configuration sections:

- **aws_config**: AWS region and profile settings
- **aurora**: RDS instance configuration
- **glue_connection**: Database connection parameters
- **glue_crawler**: Data catalog crawler settings
- **glue_job**: ETL job configuration

Update the following placeholders:
- `sg-xxxxxxxxxxxxxxxxx` - Your VPC security group IDs
- `subnet-xxxxxxxxxxxxxxxxx` - Your subnet IDs
- `your-aws-profile` - Your AWS CLI profile name

## 🎯 Usage

### Run Complete Pipeline

```bash
cd bin
python3 main.py
```

### Run Individual Components

```bash
# Create RDS instance only
python3 RDS.py

# Create S3 bucket only
python3 S3.py

# Set up IAM roles only
python3 IAM.py

# Run DMS migration only
python3 DMS.py
```

### Command Line Options

```bash
# Force recreate IAM roles
python3 main.py --force-recreate-roles

# Use existing RDS instance
python3 main.py --use-existing-rds

# Skip database initialization
python3 main.py --skip-db-init
```

## 📁 Project Structure

```
├── bin/
│   ├── main.py              # Main orchestration script
│   ├── RDS.py               # RDS MS SQL Server management
│   ├── S3.py                # S3 bucket operations
│   ├── IAM.py               # IAM role and policy management
│   ├── DMS.py               # DMS replication setup
│   ├── utils.py             # Utility functions
│   ├── clear_di.py          # Resource cleanup script
│   ├── delete_di.py         # Resource deletion script
│   ├── parameters.json      # Configuration parameters
│   ├── run_data.json        # Runtime state tracking
│   └── .env                 # Environment variables
├── table-mappings.json      # DMS table mapping rules
├── dms.md                   # DMS documentation
├── rm.md                    # Resource management notes
└── README.md               # This file
```

## 🔄 Pipeline Workflow

### Step 1: RDS MS SQL Server Setup
- Creates RDS MS SQL Server instance
- Initializes database with sample schema
- Inserts test data into `raw_src` table

### Step 2: S3 Bucket Creation
- Creates target S3 bucket with unique naming
- Sets up `dms-data/` folder structure

### Step 3: IAM Role Configuration
- Creates DMS service roles:
  - `dms-s3-access-role` - S3 access permissions
  - `dms-vpc-role` - VPC management permissions
- Attaches necessary policies

### Step 4: DMS Migration Setup
- Creates DMS replication instance
- Configures source endpoint (SQL Server)
- Configures target endpoint (S3)
- Creates and starts replication task

## 📊 Data Flow

```
SQL Server (RDS) → DMS Replication Instance → S3 Bucket
     ↓                        ↓                    ↓
  raw_src table         Data transformation    Parquet files
```

## 🛠️ Troubleshooting

### Common Issues

1. **Security Group Access**
   - Ensure security groups allow inbound traffic on port 1433 (SQL Server)
   - Verify VPC and subnet configurations

2. **IAM Permissions**
   - Check AWS credentials have sufficient permissions
   - Verify DMS service roles are properly configured

3. **Database Connection**
   - Wait for RDS instance to be fully available
   - Check database credentials and endpoint

4. **DMS Task Failures**
   - Review CloudWatch logs for detailed error messages
   - Verify table mappings configuration

### Cleanup Resources

```bash
# Clean up all created resources
python3 clear_di.py

# Delete specific resources
python3 delete_di.py
```

## 📝 State Management

The pipeline uses `run_data.json` to track created resources and enable resuming execution:

- **RDS instances** - Instance IDs and endpoints
- **S3 buckets** - Bucket names and folders
- **IAM roles** - Role ARNs
- **DMS components** - Instance, endpoint, and task ARNs

## 🔒 Security Considerations

- Store sensitive credentials in `.env` file (not committed to git)
- Use IAM roles with least privilege principle
- Enable encryption for RDS and S3 resources
- Regularly rotate access keys

## 📚 Additional Resources

- [AWS DMS Documentation](https://docs.aws.amazon.com/dms/)
- [DMS Best Practices](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_BestPractices.html)
- [Table Mapping Rules](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Tasks.CustomizingTasks.TableMapping.html)

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## 📄 License

This project is part of the AWS Data Engineering Academy curriculum.

---

**Note**: This pipeline creates AWS resources that may incur costs. Remember to clean up resources when not needed to avoid unnecessary charges.
