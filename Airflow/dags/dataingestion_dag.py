from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import urllib.request
import pandas as pd
from google.cloud import storage, bigquery

default_args = {
    'start_date': datetime(2023, 1, 1),
    'retries': 3,
    'retry_delay': 300,  # 5 minutes between retries
}

BASE_URL = "https://storage.googleapis.com/covid19-open-data/v3/"
DOWNLOAD_DIR = "."

files = ['index', 'demographics', 'economy', 'geography', 'hospitalizations', 'mobility', 
         'epidemiology', 'health', 'vaccinations']

##### CHANGE THIS #################
project_id = "cv19-453102"
bucket_name = 'cv19-453102-bucket'
dataset = 'google_open_data'
credentials = '/opt/airflow/keys/my-creds.json'
##################################


def download_all_files():
    """Downloads all CSV files and converts them to Parquet."""
    for file_name in files:
        download_file(file_name)


def download_file(file_name):
    """Downloads CSV file and converts it to Parquet."""
    url = f"{BASE_URL}{file_name}.csv"
    file_path = os.path.join(DOWNLOAD_DIR, f"{file_name}.csv")
    parquet_path = os.path.join(DOWNLOAD_DIR, f"{file_name}.parquet")

    try:
        print(f"Downloading {url}...")
        urllib.request.urlretrieve(url, file_path)
        print(f"Downloaded: {file_path}")

        print(f"Converting {file_path} to parquet...")
        df = pd.read_csv(file_path)
        df.to_parquet(parquet_path, index=False)
        os.remove(file_path)  
        print(f"Converted and removed: {file_path}")

        return parquet_path
    except Exception as e:
        print(f"Failed to download {url}: {e}")
        return None


def upload_to_gcs():
    """Uploads Parquet files to GCS."""
    storage_client = storage.Client.from_service_account_json(credentials)
    bucket = storage_client.bucket(bucket_name)

    for file in files:
        parquet_path = os.path.join(DOWNLOAD_DIR, f"{file}.parquet")
        destination_blob_name = f"raw/{file}.parquet"

        if os.path.exists(parquet_path):
            try:
                print(f"Uploading {parquet_path} to {bucket_name}...")
                blob = bucket.blob(destination_blob_name)
                blob.upload_from_filename(parquet_path)
                print(f"Uploaded {parquet_path} to gs://{bucket_name}/{destination_blob_name}")

                os.remove(parquet_path) 
                print(f"Removed {parquet_path}")
            except Exception as e:
                print(f"Failed to upload {parquet_path}: {e}")


def ingest_into_bigquery():
    """Loads Parquet files from GCS to BigQuery."""
    client = bigquery.Client.from_service_account_json(credentials)

    parquet_files = [
        ("index.parquet", "index"),
        ("demographics.parquet", "demographics"),
        ("economy.parquet", "economy"),
        ("geography.parquet", "geography"),
        ("hospitalizations.parquet", "hospitalizations"),
        ("mobility.parquet", "mobility"),
        ("epidemiology.parquet", "epidemiology"),
        ("health.parquet", "health"),
        ("vaccinations.parquet", "vaccinations"),
    ]

    for file, table in parquet_files:
        uri = f"gs://{bucket_name}/raw/{file}"

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )

        try:
            print(f"Loading {file} into {dataset}.{table}...")
            load_job = client.load_table_from_uri(uri, f"{dataset}.{table}", job_config=job_config)
            load_job.result()
            print(f"✅ Loaded {file} into {dataset}.{table}")
        except Exception as e:
            print(f"❌ Failed to load {file}: {e}")


with DAG(
    'covid_data_pipeline',
    default_args=default_args,
    schedule_interval='@monthly',
    catchup=False,
    tags=['covid', 'gcs'],
) as dag:

    download_files_task = PythonOperator(
        task_id='download_files',
        python_callable=download_all_files,  # Changed to the download function
    )

    upload_to_gcs_task = PythonOperator(
        task_id='upload_to_gcs',
        python_callable=upload_to_gcs,
    )

    ingest_to_bigquery_task = PythonOperator(
        task_id='ingest_into_bigquery',
        python_callable=ingest_into_bigquery,
    )

    # Define the correct task dependencies
    download_files_task >> upload_to_gcs_task >> ingest_to_bigquery_task
