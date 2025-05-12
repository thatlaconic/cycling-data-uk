locals {
  data_lake_bucket = "cycling_data"
}

variable "project" {
  description = "Project for Data Engineering"
  default     = "boreal-quarter-455022-q5"
  type        = string
}

variable "credentials" {
  default     = "/home/codespace/.config/gcloud/key.json"
  type        = string
}

variable "location" {
  description = "Region for GCP resources"
  default     = "asia-southeast1"
  type        = string
}

variable "storage_class" {
  description = "Storage class type for my bucket"
  default     = "STANDARD"
}

variable "BQ_DATASET" {
  description = "Cycling BigQuery Dataset"
  type        = string
  default     = "cycling_data_uk"
}