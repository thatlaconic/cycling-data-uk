#!/usr/bin/env python
# coding: utf-8

import pyspark
from pyspark.sql import SparkSession

def process_file(spark, year):
    parquet_input = f"data/processed/{year}/*"
    parquet_output = f"data/combined/{year}"
    
    # Read all parquet files for the year
    df = spark.read.parquet(parquet_input)
    
    # Write as single output (coalesce to 1 file)
    df.repartition(4).write.parquet(parquet_output, mode='overwrite')

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName('parquet-combiner') \
        .getOrCreate()
    
    try:
        year = input("Year: ")
        print(f"Combining files for year {year}")
        
        # Process the files
        process_file(spark, year)
        
        print("Files combined successfully!")
    finally:
        # Stop Spark session
        spark.stop()

if __name__ == "__main__":
    main()