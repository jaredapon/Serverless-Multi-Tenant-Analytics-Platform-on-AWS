"""
Tenant Analytics Handler Template

This template provides the base functionality for exporting tenant analytics data
to CSV and uploading to their S3 bucket. Each tenant should copy this file to
their directory and customize as needed.
"""

import os
import csv
from datetime import datetime, time, timezone, timedelta
from typing import Optional

import boto3
from sqlalchemy import create_engine, select, MetaData, Table
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("Please set DATABASE_URL in your .env")

# Initialize clients
engine = create_engine(DATABASE_URL, connect_args={"sslmode": "require"}, echo=False)
S3 = boto3.client("s3")

# Define bucket names for each tenant
BUCKET_NAMES = {
    "campus": "campus-student-lifecycle-tenant-bucket",
    "evntgarde": "evntgarde-event-management-tenant-bucket",
    "pillars": "pillars-edu-quality-assessor-tenant-bucket",
    "teleo": "teleo-church-application-tenant-bucket",
}

def get_tenant_mart(tenant: str) -> Table:
    """Get the tenant's data mart table"""
    meta = MetaData(schema="OLAP")
    return Table(f"mart_{tenant.lower()}", meta, autoload_with=engine)

def export_and_upload(date_str: Optional[str] = None, tenant: str = None, last_export_time: Optional[str] = None) -> None:
    """
    Export tenant's data mart to CSV and upload to their S3 bucket.
    
    Args:
        date_str: Optional date string in YYYY-MM-DD format. If not provided,
                 defaults to yesterday's date.
        tenant: The tenant name (e.g., 'teleo', 'pillars', etc.)
        last_export_time: Optional last export time in YYYY-MM-DDTHH:MM:SS format
    """
    if not tenant:
        raise ValueError("tenant parameter is required")
        
    # Use provided date or default to yesterday
    if not date_str:
        date_str = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
    
    # Parse date and build time window
    dt = datetime.strptime(date_str, "%Y-%m-%d").date()
    start_dt = datetime.combine(dt, time.min)
    end_dt = start_dt + timedelta(days=1)
    
    print(f"[{tenant}] Exporting data for {date_str}")
    
    # Get tenant's mart table
    mart_table = get_tenant_mart(tenant)
    
    # If last_export_time is provided, filter by it
    if last_export_time:
        last_dt = datetime.strptime(last_export_time, "%Y-%m-%dT%H:%M:%S")
        query = select(mart_table).where(mart_table.c.created_at > last_dt)
    else:
        # fallback: export for the day
        query = select(mart_table).where(
            (mart_table.c.created_at >= start_dt) &
            (mart_table.c.created_at < end_dt)
        )
    
    # Export to CSV
    csv_filename = f"{tenant.lower()}_{date_str}.csv"
    csv_path = os.path.join('/tmp', csv_filename)
    
    with engine.connect() as conn:
        result = conn.execute(query)
        rows = result.fetchall()
        
        if not rows:
            print(f"[{tenant}] No data to export for {date_str}")
            return
            
        # Write to CSV
        with open(csv_path, 'w', newline='') as f:
            writer = csv.writer(f)
            # Write header
            writer.writerow(result.keys())
            # Write data
            writer.writerows(rows)
    
    # Upload to S3
    bucket_name = BUCKET_NAMES[tenant.lower()]
    s3_key = f"analytics/{csv_filename}"
    
    try:
        S3.upload_file(csv_path, bucket_name, s3_key)
        print(f"[{tenant}] Successfully uploaded {csv_filename} to s3://{bucket_name}/{s3_key}")
    except Exception as e:
        print(f"[{tenant}] Failed to upload {csv_filename} to S3: {str(e)}")
    finally:
        # Clean up local CSV file
        if os.path.exists(csv_path):
            os.remove(csv_path)

def lambda_handler(event, context):
    """
    AWS Lambda handler function.
    
    The event can contain:
    - date_str: Optional date string in YYYY-MM-DD format
    - tenant: The tenant name (required)
    - last_export_time: Optional last export time in YYYY-MM-DDTHH:MM:SS format
    """
    date_str = event.get('date_str')
    tenant = event.get('tenant')
    last_export_time = event.get('last_export_time')
    
    if not tenant:
        raise ValueError("tenant parameter is required in the event")
    
    export_and_upload(date_str, tenant, last_export_time)
    
    return {
        'statusCode': 200,
        'body': f'Successfully processed analytics for {tenant} on {date_str or "yesterday"}'
    } 