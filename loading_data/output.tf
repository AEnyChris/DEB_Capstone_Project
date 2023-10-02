output "data_bucket_base_url" {
  value = google_storage_bucket.data_bucket.url
}

output "data_bucket_uri" {
  value = google_storage_bucket.data_bucket.self_link
}
output "scripts_bucket_base_url" {
  value = google_storage_bucket.scripts.url
}

output "scripts_bucket_uri" {
  value = google_storage_bucket.scripts.self_link
}

output "log_review_txfm_script_uri" {
  value = google_storage_bucket_object.log_reviews_transf.self_link
}

# output "movie_review_url" {
#   value = google_storage_bucket_object.movie_review.self_link
# }
