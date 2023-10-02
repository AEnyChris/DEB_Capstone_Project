resource "google_bigquery_dataset" "default" {
  dataset_id = var.dataset_id
  location   = var.region
  description = "wizeline capstone project DW" 

  labels = {
    env = "default"
  }
}