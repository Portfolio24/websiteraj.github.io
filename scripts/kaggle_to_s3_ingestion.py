"""
Kaggle to S3 Data Ingestion Pipeline - Automated Version
=========================================================
This version is designed for GitHub Actions and CI/CD pipelines.
No user prompts - all configuration via environment variables.
"""

import os
import zipfile
import json
import sys
from datetime import datetime
import pandas as pd
import boto3
from botocore.exceptions import ClientError
from requests.auth import HTTPBasicAuth
import requests
from typing import Optional, Dict

# ================= CONFIGURATION =================

class Config:
    """Configuration from environment variables"""
    
    # Required credentials
    KAGGLE_USERNAME = os.getenv('KAGGLE_USERNAME')
    KAGGLE_KEY = os.getenv('KAGGLE_KEY')
    AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
    AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
    AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')
    
    # S3 Configuration
    S3_BUCKET = os.getenv('S3_BUCKET', 'bronze-03')
    S3_PREFIX = os.getenv('S3_PREFIX', 'final-raw/')
    
    # Processing configuration
    CHUNK_ROWS = int(os.getenv('CHUNK_ROWS', '100000'))
    
    # File paths
    ZIP_PATH = "youtube.zip"
    EXTRACTED_CSV = "youtube.csv"
    SUMMARY_FILE = "ingestion_summary.json"
    
    @classmethod
    def validate(cls) -> bool:
        """Validate required environment variables"""
        required = {
            'KAGGLE_USERNAME': cls.KAGGLE_USERNAME,
            'KAGGLE_KEY': cls.KAGGLE_KEY,
        }
        
        missing = [key for key, value in required.items() if not value]
        
        if missing:
            print(f"❌ Missing required environment variables: {', '.join(missing)}")
            print("\nRequired GitHub Secrets:")
            print("  - KAGGLE_USERNAME")
            print("  - KAGGLE_KEY")
            print("  - AWS_ACCESS_KEY_ID")
            print("  - AWS_SECRET_ACCESS_KEY")
            print("  - AWS_REGION (optional, default: us-east-1)")
            return False
        return True
    
    @classmethod
    def print_config(cls):
        """Print configuration (without sensitive data)"""
        print("\n" + "="*60)
        print("Configuration")
        print("="*60)
        print(f"Kaggle Username: {cls.KAGGLE_USERNAME}")
        print(f"AWS Region: {cls.AWS_REGION}")
        print(f"S3 Bucket: {cls.S3_BUCKET}")
        print(f"S3 Prefix: {cls.S3_PREFIX}")
        print(f"Chunk Size: {cls.CHUNK_ROWS:,} rows")
        print("="*60 + "\n")

# ================= UTILITY FUNCTIONS =================

class Summary:
    """Track pipeline execution summary"""
    
    def __init__(self):
        self.start_time = datetime.utcnow()
        self.end_time = None
        self.status = "running"
        self.total_rows = 0
        self.files_uploaded = []
        self.errors = []
        self.download_size_mb = 0
    
    def complete(self, success: bool = True):
        """Mark pipeline as complete"""
        self.end_time = datetime.utcnow()
        self.status = "success" if success else "failed"
    
    def add_error(self, error: str):
        """Add error message"""
        self.errors.append({
            "timestamp": datetime.utcnow().isoformat(),
            "error": str(error)
        })
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        duration = (self.end_time - self.start_time).total_seconds() if self.end_time else 0
        
        return {
            "status": self.status,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration_seconds": round(duration, 2),
            "total_rows_processed": self.total_rows,
            "files_uploaded": len(self.files_uploaded),
            "file_list": self.files_uploaded,
            "download_size_mb": round(self.download_size_mb, 2),
            "errors": self.errors,
            "configuration": {
                "s3_bucket": Config.S3_BUCKET,
                "s3_prefix": Config.S3_PREFIX,
                "chunk_size": Config.CHUNK_ROWS,
                "aws_region": Config.AWS_REGION
            }
        }
    
    def save(self, filepath: str):
        """Save summary to JSON file"""
        with open(filepath, 'w') as f:
            json.dump(self.to_dict(), f, indent=2)
        print(f"\n📄 Summary saved to: {filepath}")

# ================= PIPELINE FUNCTIONS =================

def initialize_s3_client(config: Config) -> boto3.client:
    """Initialize S3 client"""
    # If AWS credentials are in environment (from GitHub Actions), 
    # boto3 will use them automatically
    if config.AWS_ACCESS_KEY and config.AWS_SECRET_KEY:
        return boto3.client(
            "s3",
            aws_access_key_id=config.AWS_ACCESS_KEY,
            aws_secret_access_key=config.AWS_SECRET_KEY,
            region_name=config.AWS_REGION
        )
    else:
        # Use default credentials (from AWS Actions config)
        return boto3.client("s3", region_name=config.AWS_REGION)

def verify_s3_access(s3_client: boto3.client, bucket: str) -> bool:
    """Verify S3 bucket access"""
    try:
        s3_client.head_bucket(Bucket=bucket)
        print(f"✅ S3 bucket '{bucket}' is accessible")
        return True
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            print(f"❌ S3 bucket '{bucket}' does not exist")
        elif error_code == '403':
            print(f"❌ Access denied to S3 bucket '{bucket}'")
        else:
            print(f"❌ Error accessing S3 bucket: {e}")
        return False

def download_kaggle_dataset(config: Config, summary: Summary) -> bool:
    """Download dataset from Kaggle"""
    print("⬇️  Downloading dataset from Kaggle...")
    
    url = "https://www.kaggle.com/api/v1/datasets/download/canerkonuk/youtube-trending-videos-global"
    
    try:
        with requests.get(
            url,
            auth=HTTPBasicAuth(config.KAGGLE_USERNAME, config.KAGGLE_KEY),
            stream=True,
            timeout=300
        ) as response:
            response.raise_for_status()
            
            total_size = int(response.headers.get('content-length', 0))
            downloaded = 0
            
            with open(config.ZIP_PATH, "wb") as file:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        file.write(chunk)
                        downloaded += len(chunk)
                        if total_size > 0:
                            percent = (downloaded / total_size) * 100
                            print(f"   Progress: {percent:.1f}%", end='\r')
            
            summary.download_size_mb = downloaded / (1024 * 1024)
            print(f"\n✅ Downloaded {summary.download_size_mb:.1f} MB")
            return True
            
    except requests.exceptions.RequestException as e:
        error_msg = f"Download failed: {e}"
        print(f"❌ {error_msg}")
        summary.add_error(error_msg)
        return False

def extract_csv_from_zip(config: Config, summary: Summary) -> bool:
    """Extract CSV from zip archive"""
    print("📦 Extracting CSV...")
    
    try:
        with zipfile.ZipFile(config.ZIP_PATH, 'r') as zip_ref:
            csv_files = [f for f in zip_ref.namelist() if f.endswith('.csv')]
            
            if not csv_files:
                error_msg = "No CSV file found in archive"
                print(f"❌ {error_msg}")
                summary.add_error(error_msg)
                return False
            
            csv_file = csv_files[0]
            zip_ref.extract(csv_file)
            
            if '/' in csv_file:
                os.rename(csv_file, config.EXTRACTED_CSV)
            else:
                os.rename(csv_file, config.EXTRACTED_CSV)
            
            print(f"✅ Extracted: {config.EXTRACTED_CSV}")
            return True
            
    except Exception as e:
        error_msg = f"Extraction failed: {e}"
        print(f"❌ {error_msg}")
        summary.add_error(error_msg)
        return False

def process_and_upload_data(config: Config, s3_client: boto3.client, summary: Summary) -> bool:
    """Process CSV and upload to S3"""
    print(f"⚙️  Processing in chunks of {config.CHUNK_ROWS:,} rows...\n")
    
    try:
        part_number = 1
        cutoff_date = pd.Timestamp('2026-01-19')
        
        for chunk in pd.read_csv(
            config.EXTRACTED_CSV,
            chunksize=config.CHUNK_ROWS,
            dtype=str,
            low_memory=False
        ):
            # Filter data from January 19, 2026 onwards based on video_published_at
            # Parse the ISO8601 format (2024-10-11T00:00:06Z)
            chunk['video_published_at'] = pd.to_datetime(chunk['video_published_at'], errors='coerce')
            
            # Filter rows where date is valid and >= cutoff date
            chunk = chunk[chunk['video_published_at'].dt.tz_localize(None) >= cutoff_date]
            
            # Skip empty chunks after filtering
            if len(chunk) == 0:
                continue
            
            chunk_size = len(chunk)
            summary.total_rows += chunk_size
            print(f"📊 Chunk {part_number}: {chunk_size:,} rows (filtered from 2026-01-19)")
            
            # Save temporary file
            temp_file = f"part_{part_number:04d}.csv"
            chunk.to_csv(temp_file, index=False)
            
            # Upload to S3
            s3_key = f"{config.S3_PREFIX}{temp_file}"
            try:
                s3_client.upload_file(temp_file, config.S3_BUCKET, s3_key)
                s3_url = f"s3://{config.S3_BUCKET}/{s3_key}"
                summary.files_uploaded.append(s3_url)
                print(f"   ✅ Uploaded to {s3_url}")
            except ClientError as e:
                error_msg = f"Upload failed for {temp_file}: {e}"
                print(f"   ❌ {error_msg}")
                summary.add_error(error_msg)
                os.remove(temp_file)
                return False
            
            os.remove(temp_file)
            part_number += 1
        
        print(f"\n🎉 Processed {summary.total_rows:,} total rows")
        print(f"📁 Uploaded {len(summary.files_uploaded)} files")
        return True
        
    except Exception as e:
        error_msg = f"Processing failed: {e}"
        print(f"❌ {error_msg}")
        summary.add_error(error_msg)
        return False

def cleanup_files(config: Config):
    """Remove temporary files"""
    print("\n🧹 Cleaning up...")
    
    for file_path in [config.ZIP_PATH, config.EXTRACTED_CSV]:
        if os.path.exists(file_path):
            os.remove(file_path)
            print(f"   Removed: {file_path}")

# ================= MAIN =================

def main():
    """Main pipeline execution"""
    print("\n" + "="*60)
    print("Kaggle to S3 Data Ingestion Pipeline - Automated")
    print("="*60)
    
    summary = Summary()
    config = Config()
    
    try:
        # Validate configuration
        if not config.validate():
            summary.complete(success=False)
            summary.save(config.SUMMARY_FILE)
            sys.exit(1)
        
        config.print_config()
        
        # Initialize S3
        s3_client = initialize_s3_client(config)
        
        # Verify S3 access
        if not verify_s3_access(s3_client, config.S3_BUCKET):
            summary.add_error("S3 bucket not accessible")
            summary.complete(success=False)
            summary.save(config.SUMMARY_FILE)
            sys.exit(1)
        
        # Execute pipeline
        success = True
        
        if not download_kaggle_dataset(config, summary):
            success = False
        elif not extract_csv_from_zip(config, summary):
            success = False
        elif not process_and_upload_data(config, s3_client, summary):
            success = False
        
        # Cleanup
        cleanup_files(config)
        
        # Complete summary
        summary.complete(success=success)
        summary.save(config.SUMMARY_FILE)
        
        if success:
            print("\n" + "="*60)
            print("✅ PIPELINE COMPLETED SUCCESSFULLY")
            print("="*60)
            print(f"\n📊 Statistics:")
            print(f"   Total Rows: {summary.total_rows:,}")
            print(f"   Files Uploaded: {len(summary.files_uploaded)}")
            print(f"   Duration: {summary.to_dict()['duration_seconds']:.1f} seconds")
            print(f"   Location: s3://{config.S3_BUCKET}/{config.S3_PREFIX}")
            sys.exit(0)
        else:
            print("\n" + "="*60)
            print("❌ PIPELINE FAILED")
            print("="*60)
            if summary.errors:
                print("\n🔍 Errors:")
                for error in summary.errors:
                    print(f"   - {error['error']}")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n\n⚠️  Pipeline interrupted")
        summary.add_error("Pipeline interrupted by user")
        summary.complete(success=False)
        cleanup_files(config)
        summary.save(config.SUMMARY_FILE)
        sys.exit(1)
    except Exception as e:
        error_msg = f"Unexpected error: {e}"
        print(f"\n❌ {error_msg}")
        summary.add_error(error_msg)
        summary.complete(success=False)
        cleanup_files(config)
        summary.save(config.SUMMARY_FILE)
        sys.exit(1)

if __name__ == "__main__":
    main()
