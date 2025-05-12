import os
import requests
from datetime import datetime, timedelta
from pyspark.sql import SparkSession

def format_date_for_url(date_str):
    """Convert date to URL format with abbreviated month names (e.g., 03Dec2018)"""
    date_obj = parse_custom_date(date_str)
    return date_obj.strftime("%d%b%Y")  # Using %b for abbreviated month

def parse_custom_date(date_str):
    """Handle dates and ensure we use abbreviated month names"""
    month_map = {
        'January': 'Jan', 'February': 'Feb', 'March': 'Mar', 'April': 'Apr',
        'May': 'May', 'June': 'Jun', 'July': 'Jul', 'August': 'Aug',
        'September': 'Sep', 'October': 'Oct', 'November': 'Nov', 'December': 'Dec'
    }
    
    # Try standard parsing first
    try:
        return datetime.strptime(date_str, "%d%b%Y")
    except ValueError:
        # Replace full month names with abbreviations
        for full, abbr in month_map.items():
            if full in date_str:
                date_str = date_str.replace(full, abbr)
                break
        return datetime.strptime(date_str, "%d%b%Y")

def process_file(prefix, date_range, year):
    """Download CSV and convert to Parquet using only abbreviated month names"""
    # Format dates for filename (with abbreviated months)
    start_date_str, end_date_str = date_range.split('-')
    start_date = parse_custom_date(start_date_str)
    end_date = parse_custom_date(end_date_str)
    filename_date_range = f"{start_date.strftime('%d%b%Y')}-{end_date.strftime('%d%b%Y')}"
    
    # Now using the same abbreviated format for URL
    url_filename = f"{prefix}JourneyDataExtract{filename_date_range}.csv"
    filename = f"{prefix}JourneyDataExtract{filename_date_range}.csv"
    csv_path = f"data/raw/{year}/{filename}"
    parquet_path = f"data/processed/{year}/{prefix}-{filename_date_range}"
    
    # Skip if Parquet already exists
    if os.path.exists(parquet_path):
        print(f"Parquet exists: {filename}")
        return True

    # 1. Download CSV if needed
    if not os.path.exists(csv_path):
        url = f"https://cycling.data.tfl.gov.uk/usage-stats/{url_filename}"
        try:
            print(f"Downloading {filename}...")
            
            # First check if file exists
            head_response = requests.head(url, timeout=10)
            if head_response.status_code != 200:
                print(f"File not found at {url}")
                return False
                
            os.makedirs(os.path.dirname(csv_path), exist_ok=True)
            with requests.get(url, stream=True, timeout=30) as r:
                r.raise_for_status()
                with open(csv_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
        except Exception as e:
            print(f"Download failed: {filename} - {str(e)}")
            return False

    # 2. Convert to Parquet
    spark = None
    try:
        spark = SparkSession.builder.getOrCreate()
        df = spark.read.option("header", "true").csv(csv_path)
        df.write.parquet(parquet_path, mode='overwrite')
        print(f"Converted to Parquet: {filename}")
        return True
    except Exception as e:
        print(f"Conversion failed: {filename} - {str(e)}")
        return False
    finally:
        if spark:
            spark.stop()
        # 3. Delete CSV only if Parquet was created
        if os.path.exists(parquet_path) and os.path.exists(csv_path):
            os.remove(csv_path)
            print(f"Deleted CSV: {filename}")

def main():
    print("TfL Data Processor (CSV → Parquet)")
    print("Enter dates with either full or abbreviated month names (e.g., 01May2018 or 01July2018)")
    
    prefix_start = int(input("Start prefix (1-500): "))
    prefix_end = int(input("End prefix (1-500): "))
    date_start = input("Start date (e.g., 01May2018 or 01July2018): ")
    date_end = input("End date (e.g., 31May2018 or 31July2018): ")
    year = input("Year: ")

    # Create date ranges using custom parser
    start_date = parse_custom_date(date_start)
    end_date = parse_custom_date(date_end)
    current_date = start_date
    
    while current_date <= end_date:
        week_end = min(current_date + timedelta(days=6), end_date)
        date_range = f"{current_date.strftime('%d%b%Y')}-{week_end.strftime('%d%b%Y')}"
        
        for prefix in range(prefix_start, prefix_end + 1):
            process_file(prefix, date_range, year)
        
        current_date = week_end + timedelta(days=1)

if __name__ == "__main__":
    main()