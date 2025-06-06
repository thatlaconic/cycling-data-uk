# cycling-data-uk

## Preparing the Environment with Github Codespaces
1. **Installing Terraform**
    - installing from this [website](https://developer.hashicorp.com/terraform/downloads)
```bash
wget -O - https://apt.releases.hashicorp.com/gpg | sudo gpg --dearmor -o /usr/share/keyrings/hashicorp-archive-keyring.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/hashicorp-archive-keyring.gpg] https://apt.releases.hashicorp.com $(lsb_release -cs) main" | sudo tee /etc/apt/sources.list.d/hashicorp.list
sudo apt update && sudo apt install terraform
```
2. **Install jupyter notebook**
```bash
pip install jupyter notebook
```
3. **Setting up service account in Google Cloud**
    - go to google cloud > IAM & Admin > Service Accounts > Create service account > create new key

4. **Using Codespace Secret**
    - go to repository that has codespace > settings > secrets and variables > codespaces

5. **Download [SDK](https://cloud.google.com/sdk/docs/install-sdk) for local setup**

6. Create the File in Codespace. Run these commands in the terminal:
```bash
Copy
# Create the .config/gcloud directory (if it doesn't exist)
mkdir -p ~/.config/gcloud

# Write the secret to key.json
echo "$GOOGLE_CREDENTIALS_JSON" > ~/.config/gcloud/key.json

# Set strict permissions (required for security)
chmod 600 ~/.config/gcloud/key.json
```
**Verify the Key**

```bash
Copy
# Check if the file exists and has content
cat ~/.config/gcloud/key.json

# Validate JSON format (install jq if needed: sudo apt-get install jq)
jq '.' ~/.config/gcloud/key.json
```
**Authenticate with Google Cloud**

```bash
Copy
# Activate the service account
gcloud auth activate-service-account --key-file=/home/codespace/.config/gcloud/key.json

# Verify authentication
gcloud config list
Troubleshooting
cat shows nothing?
```
**Check if the secret was loaded:**

```bash
Copy
echo "Length of secret: ${#GOOGLE_CREDENTIALS_JSON}"
```
If 0, restart the Codespace or re-add the secret.

## Installing PySpark

+ Instructions [here](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/05-batch/setup/linux.md)

+ Download : wget https://dlcdn.apache.org/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3.tgz

export JAVA_HOME="${HOME}/.config/spark/jdk-11.0.2"
export PATH="${JAVA_HOME}/bin:${PATH}"

export SPARK_HOME="${HOME}/.config/spark/spark-3.5.5-bin-hadoop3"
export PATH="${SPARK_HOME}/bin:${PATH}"

+ paste this before running jupyter:

```bash 
export PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"
export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH"
```

## Installing Hadoop (GCS connector)
gsutil cp gs://hadoop-lib/gcs/gcs-connector-hadoop3-2.2.5.jar gcs-connector-hadoop3-2.2.5.jar
