echo "GCP_BUCKET_NAME=cycling_data_boreal-quarter-455022-q5" >> .env
echo "GCP_DATASET=cycling_data_uk" >> .env
echo "GCP_PROJECT_ID=boreal-quarter-455022-q5" >> .env
echo "GCP_LOCATION=asia-southeast1" >> .env

python NI.py --prefix-start 140 --prefix-end 142 --date-start 12Dec2018 --date-end 01Jan2019 --year 2018