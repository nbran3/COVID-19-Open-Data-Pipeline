from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2023, 1, 1),
    'retries': 3,
    'retry_delay': 300,  # 5 minutes between retries
}

with DAG(
    'covid_data_pipeline', 
    default_args=default_args, 
    schedule_interval='@daily', 
    catchup=False,
    tags=['covid', 'gcs']
) as dag:
    
    # Create a Python script file that Airflow will execute
    create_script = BashOperator(
        task_id='create_script',
        bash_command="""
cat > /tmp/covid_data_script.py << 'EOL'
import os
import urllib.request
from google.cloud import storage

BASE_URL = "https://storage.googleapis.com/covid19-open-data/v3/"
DOWNLOAD_DIR = "/tmp"  # Using /tmp in Airflow container

files = ['index', 'demographics', 'economy', 'geography', 'hospitalizations', 'mobility', 
         'google-search-trends', 'lawatlas-emergency-declarations', 'epidemiology', 
         'health', 'vaccinations', 'weather', 'by-age', 'by-sex']

project_id = "cv19-453102"
bucket_name = 'cv19-453102-bucket'

def download_files(file_name):
    url = f"{BASE_URL}{file_name}.csv"
    file_path = os.path.join(DOWNLOAD_DIR, f"{file_name}.csv")

    try: 
        print(f"Downloading {url}...")
        urllib.request.urlretrieve(url, file_path) 
        print(f"Downloaded: {file_path}")
        return file_path
    
    except Exception as e:
        print(f"Failed to download {url}: {e}")
        return None
    

def upload_to_gcs():
    # Use correct path for Airflow container
    storage_client = storage.Client.from_service_account_json(
        "/opt/airflow/keys/my-creds.json"
    )
    bucket = storage_client.bucket(bucket_name)

    for file in files:
        file_path = download_files(file)
        destination_blob_name = f"raw/{file}.csv"

        if file_path:  
            try:
                print(f"Ingesting {file_path} into GCS Bucket...")
                blob = bucket.blob(destination_blob_name)
                blob.upload_from_filename(file_path)

                print(f"File {file_path} uploaded to gs://{bucket_name}/{destination_blob_name}")

                os.remove(file_path)
                print(f"Removed {file_path}")

            except Exception as e:
                print(f"Failed to process {file_path}: {e}")

if __name__ == "__main__":
    upload_to_gcs()
EOL
        """
    )
    
    # Run the script
    run_script = BashOperator(
        task_id='run_script',
        bash_command="python /tmp/covid_data_script.py"
    )
    
    create_script >> run_script
