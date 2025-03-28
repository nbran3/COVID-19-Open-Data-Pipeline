# COVID-19-Open-Data-Pipeline


## This project aims to unify and transform COVID-19 data from [Google's COVID-19 Open Data Repository](https://health.google.com/covid-19/open-data/).
  
## Tech Stack
- Python
- Google Cloud (or any Cloud provider, but I used GCS for this project)
- Docker
- dbt (preferably Cloud)
- Terraform (not a must, but for the project requirements)


## Project Scope
For this project, I decided to use the Google COVID-19 Open Data Repository for my final project in the Data Engineering Zoomcamp. I felt the aggregation of the data that they provided was not very helpful if you were looking for COVID data, it is a rather slow dataset. Some of the data, like the weather file, is really useful as this was a multi-year worldwide pandemic; other data such as the search trends, were so large that I felt it was not necessary to try to dig into COVID data. For full reference, I used epidemiology, hospitalizations, mobility, vaccination, index, economy, geography, demographics, and the health files.

I tried to make the project as simple as possible, and so you need to run the Dockerfile, wait for it to build, then run an Airflow dag, one dbt command, then a final Airflow dag for the dashboard for the project to be completed. I understand that the project is not fully orchestrated, but in order to do so would heavily harm ease of use / reproducibility  


## Data Ingestion 

To start, you will need to have a Cloud account and a new project with a service account (with credentials) that allows you to store data in a Bucket/ Data Lake, the ability to read and write to BigQuery/ Cloud SQL database, and Looker/ data viz tool admin. I used Google Cloud for this project, but I am sure Azure or AWS will work rather similarly.
I mounted the credentials (JSON) in the Airflow subdirectory so there would not be any problems and hard-coded the JSON credentials to both the BigQuery and Storage client because I was having trouble passing the credentials to Airflow. 

Below are the three I used in Google Cloud :

![image](https://github.com/user-attachments/assets/4e3ee3a4-6640-4258-a9c2-64c5d2b7770d)


You will also need to either authenticate to Google Cloud or your choice of a Cloud provider either through the Google CLI, or if you run a Python Script (non Airflow), the local terminal (VSCode) will prompt you if you want to sign in/ authenitcate through the web browser to Google Cloud SDK. You will need a bucket and a dataset, I used Terraform to create the bucket and the BigQuery table by using the project ID. You will need to go into the Airflow DAG folder to switch the dataset ID, bucket ID, and the project ID, but they are Python variables, so you will only need to do it once. Below is where in the script/ dag you need to change for the project to run.

![image](https://github.com/user-attachments/assets/c3a8dcb0-d40b-44c9-b4a4-cb6731c989b0)


Make sure to either hard-code the credentials or hard-code them into both the storage and BigQuery client. All things that need to be changed are marked by "############################"

![Uploading image.png…]()


Once you have done that, all you really have to do is run the Dockerfile (which contains Airflow), wait for it to build, and then go to localhost:8080 in a browser. The login and password are both "airflow", and the specific DAG you are looking for is called "covid_data_pipeline". It will take some time to process as the script downloads a file via an iterating list, transforms it to a parquet file, uploads the parquet to a Google Cloud Bucket, then takes the data from the bucket and makes it into individual BigQuery tables, and removes the downloads CSV file from the local directory. It took me about 4 minutes and 23 seconds on my Windows desktop which is hard-wired to my router. I also did the process on my 2017 MacBook (Intel) and it took " " minutes.  


## Data Transformation
For this part of the project, I would highly recommend using dbt Cloud, but you can use dbt core, but you will need to manually change about 15 SQL files, specifically the BigQuery tables that the files are reading. In dbt Cloud, a reference system is used, so if you connect to the database that you stored the data in, dbt will automatically reference the table and you will not need to manually type out each project id. 

To do both connections on dbt Cloud, go to the "Dashboard" page and click on settings in the top right 
![image](https://github.com/user-attachments/assets/fc8d792e-e7ee-44e4-8add-42f383fb879a)

Then, on the pop-up menu, complete both the "Repository" and "Deployment connection" options. I found it easiest to manually log into GitHub and give dbt access to the specific repository. For the deployment connection, you can use the same JSON service account credentials that were generated earlier in the project. It should look like this.
![image](https://github.com/user-attachments/assets/9505c117-40d5-4fb8-b2b7-8e8ebd28db8c)


Make sure you are in the "dbtFolder" subdirectory in dbt. If both are properly loaded, all you need to do is type in "dbt run" in the CLI, and everything will run. Below is a picture of where to type in the dbt run command, and also a picture of the lineage. 
![image](https://github.com/user-attachments/assets/9c0eb17c-c5ed-40ba-b093-739d5b795021)

## Dashboards
https://lookerstudio.google.com/embed/reporting/a8aec908-8d66-4988-b50d-74d4f5a775bb/page/jkxEF



## Explanation of Dockerfile
When you download/ clone the repository onto your local machine, all you need to do is run "docker compose up --build". The Dockerfile will download Airflow and will also download and install three Python libraries needed for the task from requirements.txt using pip. The three libraries are apache-airflow, pandas, and google-cloud-storage nad bigquery. To access Airflow once the Docker image has been built, go to a browser and type in "localhost:8080", the first page should be the DAG page, and click on the dataingestion_dag and run it (play button).

## Explanation of Python script

The script is inside a BashOperator Airflow DAG. I was having trouble authenticating to Google Cloud using a PythonOperator on Windows, so I decided to just use BashOperator. The script itself takes a base URL: "https://storage.googleapis.com/covid19-open-data/v3/" and iterates through a list of terms that are each file name that we need for the project. Each iteration downloads a CSV, transforms the CSV file into a Parquet, uploads the Parquet file to an individual Google Cloud Bucket, and then takes the file in the bucket and ingests the data into BigQuery



