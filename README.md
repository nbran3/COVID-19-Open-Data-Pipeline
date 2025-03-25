# COVID-19-Open-Data-Pipeline


## This project aims to unify and transform COVID-19 data from [Google's COVID-19 Open Data Repository](https://health.google.com/covid-19/open-data/).
  
## Tech Stack
- Python
- Google Cloud (or any Cloud provider, but I used GCS for this project)
- Docker
- dbt (preferably Cloud)
- Terraform (not a must, but for the project requirements)


## Project Scope
For this project, I decided to use the Google COVID-19 Open Data Repository for my final project in the Data Engineering Zoomcamp. I felt the aggregation of the data that they provided was not very helpful if you were looking for COVID data, it is a rather slow dataset. Some of the data, like the weather file, is really not relevant as this was a multi-year worldwide pandemic; other data such as the search trends, were so large that I felt it was not necessary to try to dig into COVID data. For full reference, I used the epidemiology, hospitalizations, mobility, vaccination, index, economy, geography, demographics, and the health files.

I tried to make the project as simple as possible, and so you need to run the Dockerfile, wait for it to build, then run an Airflow dag, one dbt command, then a final Airflow dag for the dashboard for the project to be completed.


## Data Ingestion 

To start, you will need to have a Cloud account and a new project with a service account (with credentials) that allows you to store data in a Bucket/ Data Lake and the ability to read and write to BigQuery/ Cloud SQL database. I used Google Cloud for this project, but I am sure Azure or AWS will work rather similar.
I mounted the credentials (JSON) in the Airflow subdirectory so there would not be any problems. 

You will need a bucket and a dataset, I used Terraform to create the bucket and the BigQuery table by using the project ID. You will need to go into the Airflow DAG folder to switch the dataset ID, bucket ID, and the project id, but they are Python variables, so you will only need to do it once. You will also need to either authenticate to Google Cloud or your choice of a Cloud provider either through the Google CLI, or if you run a Python Script (non Airflow), the local terminal (VSCode) will prompt you if you want to sign in/ authenitcate through the web browser to Google Cloud. 

Once you have done that, all you really have to do is run the Dockerfile (which contains Airflow), wait for it to build, and then go to localhost:8080. The login and password are both airflow, and the specific DAG you are looking for is called "dataingestion_dag". It will take some time to process as the script downloads a file via an iterating list, transforms it to a parquet file, uploads the parquet to a Google Cloud Bucket, then takes the data from the bucket and makes it into individual BigQuery tables, and removes the downloads CSV file from the local directory. It took me about " " minutes on my Windows desktop that is hard wired to my router. I also did the process on my 2017 MacBook (Intel) and it took " " minutes.  


## Data Transformation


