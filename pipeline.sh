#!/bin/bash
set -e  # Exit on any error

# === Environment Setup (for PySpark to work) ===
export PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"
export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH"

# === Configuration ===
PREFIX_START=142
PREFIX_END=143
DATE_START="26Dec2018"
DATE_END="08Jan2019"
YEAR="2019"

# GCS Configuration
GCS_BUCKET="gs://cycling_data_boreal-quarter-455022-q5"
LOCAL_PROCESSED_PATH="data/processed/$YEAR"
GCS_UPLOAD_PATH="$GCS_BUCKET/processed/$YEAR"

# === STEP 1: Run Python script to process and convert CSVs ===
echo "Step 1: Downloading and processing TfL data..."
python3 ./python_scripts/01_download-convert.py \
  --prefix-start $PREFIX_START \
  --prefix-end $PREFIX_END \
  --date-start $DATE_START \
  --date-end $DATE_END \
  --year $YEAR
echo "✅ Step 1 complete."

# === STEP 2: Upload processed Parquet files to GCS ===
echo "Step 2: Uploading processed files to GCS..."
gsutil -m cp -r "$LOCAL_PROCESSED_PATH"/* "$GCS_UPLOAD_PATH"
echo "✅ Step 2 complete."

# === STEP 3: Combine Parquet files in GCS ===
echo "Step 3: Combining Parquet files in GCS..."
python3 ./python_scripts/02_combine-to-gcs.py <<EOF
$YEAR
EOF
echo "✅ Step 3 complete."

# === STEP 4: GCS to Bigquery ===
echo "gcs to bigquery in progress.."
bq query --use_legacy_sql=false < cycling_query.sql \
--project_id=boreal-quarter-455022-q5

echo "✅ Step 4 complete."

# === STEP 5: DBT Transformation ===
echo "DBT In-progress.."
dbt run --project-dir ./dbt_cycling
echo "✅ Step 5 complete."

echo "🎉 All steps finished successfully!"

