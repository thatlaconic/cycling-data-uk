import os
import sys
import requests
from pyspark.sql import SparkSession

# Get command line arguments
data_prefix = sys.argv[1]
date = sys.argv[2]
year = sys.argv[3]

# Setup paths
base_url = f"https://cycling.data.tfl.gov.uk/usage-stats/{data_prefix}JourneyDataExtract{date}.csv"
filename = f"{data_prefix}-{date}JourneyDataExtract.csv"
csv_path = os.path.join("data", "raw", year, filename)
#parquet_path = os.path.join("data", "processed", year, f"{data_prefix}-{date}.parquet")

# Download file
os.makedirs(os.path.dirname(csv_path), exist_ok=True)
with requests.get(f"{base_url}", stream=True) as r:
    with open(csv_path, 'wb') as f:
        for chunk in r.iter_content(chunk_size=8192):
            f.write(chunk)

# Convert to Parquet
#spark = SparkSession.builder.getOrCreate()
#df = spark.read.option("header", "true").csv(csv_path)
#df.write.parquet(parquet_path, mode='overwrite')
#spark.stop()

# Delete original
#os.remove(csv_path)