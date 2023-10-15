output "region" {
  value       = var.region
  description = "GCloud Region"
}

output "location" {
  value       = var.location
  description = "GCloud Region"
}

output "project_id" {
  value       = var.project_id
  description = "GCloud Project ID"
}

output "kubernetes_cluster_name" {
  value       = module.gke.kubernetes_cluster_name
  description = "GKE Cluster Name"
}

output "kubernetes_cluster_host" {
  value       = module.gke.kubernetes_cluster_host
  description = "GKE Cluster Host"
}

output "bq_dataset_id" {
  value = module.bigquery.dataset_id
}

output "bq_dataset_creation_time" {
  value = module.bigquery.creation_time
}

output "gcs_data_bucket_uri" {
  value = module.cloud-storage.data_bucket_base_url
}

output "gcs_stage_bucket_uri" {
  value = module.cloud-storage.stage_bucket_base_url
}

output "gcs_scripts_bucket_uri" {
  value = module.cloud-storage.scripts_bucket_base_url
}

output "instance_ip_address" {
  value = module.cloudsql.instance_ip_address
}