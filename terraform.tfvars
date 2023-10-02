project_id = "sodium-mountain-396818"
region     = "us-east1"
location     = "us-east1-b"

#GKE
gke_num_nodes = 2
machine_type  = "n1-standard-1"

#CloudSQL
instance_name     = "data-bootcamp-1"
database_version  = "POSTGRES_12"
instance_tier     = "db-f1-micro"
disk_space        = 10
database_name     = "dbname"
db_username       = "dbuser"
db_password       = "dbpassword"

#BigQuery
dataset_id = "deb_capstone_dw"