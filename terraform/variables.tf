variable "credentials" {
  description = "GCP credentials"
  default     = "cv19pipeline/airflow/keys/my-creds.json"

}

variable "project" {
  description = "GCP Project ID"
  default     = "cv19-453102"
}

variable "location" {
  description = "GCP Project Location"
  default     = "US"
}

variable "region" {
  description = "GCP Region Name"
  default     = "us-central1"
}

variable "bq_dataset_name" {
  description = "BigQuery dataset name"
  default     = "demo_dataset"

}

variable "gcs_bucket_name" {
  description = "GCP Storage Bucket Name"
  default     = "cv19-453102-bucket"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}
