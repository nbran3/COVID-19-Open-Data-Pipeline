import os
import urllib.request
import pandas as pd
import pandas_gbq

BASE_URL = "https://storage.googleapis.com/covid19-open-data/v3/"
DOWNLOAD_DIR = "."

files = ['index', 'demographics', 'economy', 'geography', 'hospitalizations', 'mobility', 'search-trends', 
         'epidemiology', 'health', 'vaccinations', 'vaccination-access', 'vaccination-search', 'weather']

destination_table = "test"
project_id = "test"

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
    

def upload_to_bigquery():
    for file in files:
        file_path = download_files(file)
        
        if file_path:  # Only process if download was successful
            try:
                df = pd.read_csv(file_path)
                print(f"Loaded {file_path} into a dataframe...")
                print(f"Ingesting {file_path} into BigQuery...")

                ### Uncomment when ready to upload to BigQuery
                ### pandas_gbq.to_gbq(df, destination_table=destination_table, project_id=project_id, if_exists="append")

                print(f"Uploaded {file_path} to BigQuery -> {destination_table}")

                os.remove(file_path)
                print(f"Removed {file_path}")

            except Exception as e:
                print(f"Failed to process {file_path}: {e}")
