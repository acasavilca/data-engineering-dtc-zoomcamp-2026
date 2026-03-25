variable "credentials" {
  description = "My Credentials"
  default     = "/home/andres/.gcp/dtc-de-course-491308-cbc137b6d078.json"
}

variable "project" {
  description = "Project"
  default     = "dtc-de-course-491308"
}

variable "region" {
  description = "Project region"
  default     = "us-east1"
}

variable "location" {
  description = "Project Location"
  default     = "US"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  default     = "demo_dataset"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "dtc-de-course-491308-terra-bucket"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}