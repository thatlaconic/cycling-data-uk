import os
import requests
import argparse
from datetime import datetime, timedelta
from pyspark.sql import SparkSession

# Constants
BASE_URL = "https://cycling.data.tfl.gov.uk/usage-stats/"
DATA_PREFIX = "JourneyDataExtract"
RAW_DATA_PATH = "data/raw"
PROCESSED_DATA_PATH = "data/processed"

def parse_custom_date(date_str):
    """Handle dates with either full or abbreviated month names"""
    month_map = {
        'January': 'Jan', 'February': 'Feb', 'March': 'Mar', 'April': 'Apr',
        'May': 'May', 'June': 'Jun', 'July': 'Jul', 'August': 'Aug',
        'September': 'Sep', 'October': 'Oct', 'November': 'Nov', 'December': 'Dec'
    }
    
    try:
        return datetime.strptime(date_str, "%d%b%Y")
    except ValueError:
        for full, abbr in month_map.items():
            if full in date_str:
                date_str = date_str.replace(full, abbr)
                break
        return datetime.strptime(date_str, "%d%b%Y")

def format_date_range(start_date, end_date):
    """Format date range for filenames"""
    return f"{start_date.strftime('%d%b%Y')}-{end_date.strftime('%d%b%Y')}"

def download_file(url, filepath):
    """Download a file with proper error handling"""
    try:
        head_response = requests.head(url, timeout=10)
        if head_response.status_code != 200:
            return False
            
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with requests.get(url, stream=True, timeout=30) as r:
            r.raise_for_status()
            with open(filepath, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        return True
    except Exception:
        return False

def convert_to_parquet(csv_path, parquet_path):
    """Convert CSV to Parquet using Spark"""
    spark = None
    try:
        spark = SparkSession.builder.getOrCreate()
        df = spark.read.option("header", "true").csv(csv_path)
        df.write.parquet(parquet_path, mode='overwrite')
        return True
    except Exception:
        return False
    finally:
        if spark:
            spark.stop()

def process_file(prefix, date_range, year):
    """Process a single file"""
    filename_date_range = format_date_range(*date_range)
    url_filename = f"{prefix}{DATA_PREFIX}{filename_date_range}.csv"
    filename = f"{prefix}{DATA_PREFIX}{filename_date_range}.csv"
    
    csv_path = os.path.join(RAW_DATA_PATH, year, filename)
    parquet_path = os.path.join(PROCESSED_DATA_PATH, year, f"{prefix}-{filename_date_range}")
    
    if os.path.exists(parquet_path):
        return True

    if not os.path.exists(csv_path):
        url = os.path.join(BASE_URL, url_filename)
        if not download_file(url, csv_path):
            return False

    if not convert_to_parquet(csv_path, parquet_path):
        return False

    if os.path.exists(parquet_path) and os.path.exists(csv_path):
        os.remove(csv_path)
    
    return True

def process_date_range(prefix_start, prefix_end, date_start, date_end, year):
    """Process all files in the specified range"""
    start_date = parse_custom_date(date_start)
    end_date = parse_custom_date(date_end)
    current_date = start_date
    
    while current_date <= end_date:
        week_end = min(current_date + timedelta(days=6), end_date)
        date_range = (current_date, week_end)
        
        for prefix in range(prefix_start, prefix_end + 1):
            process_file(prefix, date_range, year)
        
        current_date = week_end + timedelta(days=1)

def main():
    """Main function with argument parsing"""
    parser = argparse.ArgumentParser(description='TfL Data Processor (CSV → Parquet)')
    parser.add_argument('--prefix-start', type=int, required=True, help='Start prefix (1-500)')
    parser.add_argument('--prefix-end', type=int, required=True, help='End prefix (1-500)')
    parser.add_argument('--date-start', type=str, required=True, help='Start date (e.g., 01May2018 or 01July2018)')
    parser.add_argument('--date-end', type=str, required=True, help='End date (e.g., 31May2018 or 31July2018)')
    parser.add_argument('--year', type=str, required=True, help='Year of data')
    
    args = parser.parse_args()
    
    if not (1 <= args.prefix_start <= 500) or not (1 <= args.prefix_end <= 500):
        return
    
    if args.prefix_start > args.prefix_end:
        return
    
    process_date_range(
        args.prefix_start,
        args.prefix_end,
        args.date_start,
        args.date_end,
        args.year
    )

if __name__ == "__main__":
    main()