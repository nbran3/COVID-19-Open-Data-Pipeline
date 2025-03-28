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
    schedule_interval='@monthly', 
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
import pandas as pd
from google.cloud import storage, bigquery

BASE_URL = "https://storage.googleapis.com/covid19-open-data/v3/"
DOWNLOAD_DIR = "."

files = ['index', 'demographics', 'economy', 'geography', 'hospitalizations', 'mobility', 
          'epidemiology', 'health', 'vaccinations']


##### CHANGE THIS #################
project_id = "cv19-453102"
bucket_name = 'cv19-453102-bucket'
dataset = 'google_open_data'
##################################

def download_files(file_name):
    url = f"{BASE_URL}{file_name}.csv"
    file_path = os.path.join(DOWNLOAD_DIR, f"{file_name}.csv")
    parquet_path = os.path.join(DOWNLOAD_DIR, f"{file_name}.parquet")

    try: 
        print(f"Downloading {url}...")
        urllib.request.urlretrieve(url, file_path) 
        print(f"Downloaded: {file_path}")

        print(f"Converting {file_path} to parquet....")
        
        df = pd.read_csv(file_path)
        
        df.to_parquet(parquet_path, index=True)
        os.remove(file_path)
        print(f"Removed {file_path}")

        return parquet_path
    
    except Exception as e:
        print(f"Failed to download {url}: {e}")
        return None
    

def upload_to_gcs():
    
    #########################################################
    storage_client = storage.Client.from_service_account_json(
    "/opt/airflow/keys/my-creds.json"
    ##########################################################
)
    bucket = storage_client.bucket(bucket_name)

    for file in files:
        parquet_path = download_files(file)
        destination_blob_name = f"raw/{file}.parquet"

        if parquet_path:  
            try:
                print(f"Ingesting {parquet_path} into GCS Bucket...")

               
                blob = bucket.blob(destination_blob_name)
                blob.upload_from_filename(parquet_path)

                print(f"File {parquet_path} uploaded to gs://{bucket_name}/{destination_blob_name}")

                os.remove(parquet_path)
                print(f"Removed {parquet_path}")

            except Exception as e:
                print(f"Failed to process {parquet_path}: {e}")

upload_to_gcs()


from google.cloud import bigquery

def ingest_into_bigquery():
    ####################################################################################
    client = bigquery.Client.from_service_account_json("/opt/airflow/keys/my-creds.json")
    #####################################################################################

    parquet_files = [
        ("index.parquet", "index"),
        ("demographics.parquet", "demographics"),
        ("economy.parquet", "economy"),
        ("geography.parquet", "geography"),
        ("hospitalizations.parquet", "hospitalizations"),
        ("mobility.parquet", "mobility"),
        ("lawatlas-emergency-declarations.parquet", "lawatlas"),
        ("epidemiology.parquet", "epidemiology"),
        ("health.parquet", "health"),
        ("vaccinations.parquet", "vaccinations"),
        ("weather.parquet", "weather"),

    ]
    
    for file, table in parquet_files:
        uri = f"gs://{bucket_name}/raw/{file}"  

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )

        try:
            load_job = client.load_table_from_uri(uri, f"{dataset}.{table}", job_config=job_config)
            load_job.result()  

            print(f"✅ Loaded {file} into {dataset}.{table}")
        except Exception as e:
            print(f"❌ Failed to load {file}: {e}")

ingest_into_bigquery()

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

