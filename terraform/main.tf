provider "google" {
    project = var.project
    credentials = "${file("../keys/dvd-rental-gcp-key.json")}"
    region = var.region
    zone = var.zone
}

resource "google_storage_bucket" "my_bucket" {
  name = "dvdrental_project"
  location = var.region
  storage_class = "standard"
  public_access_prevention = "enforced"
}

resource "google_bigquery_dataset" "temp_table" {
  dataset_id                  = "temp_table"
  description                 = "Temporary data before upserting into raw table"
  location                    = var.region
  default_table_expiration_ms = 3600000

  access {
    role   = "owner"
    user_by_email = var.email
  }
}

resource "google_bigquery_dataset" "raw_dvdrental" {
  dataset_id                  = "raw_dvdrental"
  description                 = "Unprocessed data ingested from various source systems or database"
  location                    = var.region

  access {
    role   = "owner"
    user_by_email = var.email
  }
}

resource "google_bigquery_dataset" "bv" {
  dataset_id                  = "bv"
  description                 = "Store transformed data or processed data"
  location                    = var.region

  access {
    role   = "owner"
    user_by_email = var.email
  }
}

resource "google_bigquery_dataset" "public" {
  dataset_id                  = "public"
  description                 = "Data marts to be used by end users and analytics"
  location                    = var.region

  access {
    role   = "owner"
    user_by_email = var.email
  }
}

