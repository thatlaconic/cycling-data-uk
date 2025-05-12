#!/usr/bin/env python
# coding: utf-8

import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext

credentials_location = '/home/codespace/.config/gcloud/key.json'

conf = SparkConf() \
    .setMaster('local[*]') \
    .setAppName('test') \
    .set("spark.jars", "/home/codespace/.config/gcs-connector-hadoop3-2.2.5.jar") \
    .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_location)

sc = SparkContext(conf=conf)

hadoop_conf = sc._jsc.hadoopConfiguration()

hadoop_conf.set("fs.AbstractFileSystem.gs.impl",  "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", credentials_location)
hadoop_conf.set("fs.gs.auth.service.account.enable", "true")

def process_file(spark, year):
    parquet_input = f"gs://cycling_data_boreal-quarter-455022-q5/processed/{year}/*"
    parquet_output = f"gs://cycling_data_boreal-quarter-455022-q5/combined/{year}"
    
    # Read all parquet files for the year
    df = spark.read.parquet(parquet_input)
    
    # Write as single output (coalesce to 1 file)
    df.repartition(4).write.parquet(parquet_output, mode='overwrite')

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .config(conf=sc.getConf()) \
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