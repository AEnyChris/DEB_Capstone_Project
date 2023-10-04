# Create bucket
resource "google_storage_bucket" "data_bucket" {
  name          = "${var.project_id}-data-bucket"
  location      = var.region
  force_destroy = true
}

resource "google_storage_bucket" "stage_bucket" {
  name          = "${var.project_id}-stage-bucket"
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
  source = "./loading_data/pyspark_jobs/log_review_trans.py"
  bucket = google_storage_bucket.scripts.name
}

resource "google_storage_bucket_object" "movie_review_transf" {
  name   = "movie_review_txfm_script.py"
  source = "./loading_data/pyspark_jobs/movie_review_transf.py"
  bucket = google_storage_bucket.scripts.name
}

resource "google_storage_bucket_object" "create_fact_table_query" {
  name   = "create_fact_table.sql"
  source = "./loading_data/sql_queries/create_fact_table.sql"
  bucket = google_storage_bucket.scripts.name
}
