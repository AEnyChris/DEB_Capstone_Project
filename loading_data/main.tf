# Create bucket
terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "3.77.0"
    }
  }
  required_version = ">= 0.13.0"
}


resource "google_storage_bucket" "data_bucket" {
  name          = "${var.project_id}-data-bucket"
  location      = var.region
  force_destroy = true
}

resource "google_storage_bucket" "scripts" {
  name          = "${var.project_id}-scripts-bucket"
  location      = var.region
  force_destroy = true
}


resource "google_storage_bucket_object" "log_reviews_transf" {
  name   = "log_review_txfm_script.py"
  source = "./pyspark_jobs/log_review_trans.py"
  bucket = google_storage_bucket.scripts.name
}

# Load files into storage bucket: raw-data folder
# resource "google_storage_bucket_object" "log_review" {
#   name   = "raw-data/log_reviews.csv"
#   source = "data/log_reviews.csv"
#   bucket = google_storage_bucket.data_bucket.name
# }

# resource "google_storage_bucket_object" "movie_review" {
#   name   = "raw-data/movie_review.csv"
#   source = "data/movie_review.csv"
#   bucket = google_storage_bucket.data_bucket.name
# }

# resource "google_storage_bucket_object" "user_purchase" {
#   name   = "raw-data/user_purchase.csv"
#   source = "data/user_purchase.csv"
#   bucket = google_storage_bucket.data_bucket.name
# }