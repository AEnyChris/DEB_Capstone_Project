project_id = "sodium-mountain-396818"
region     = "us-east1"
location   = "us-east1-b"

#GKE
gke_num_nodes = 2
machine_type  = "n1-standard-1"

#CloudSQL
instance_name         = "data-bootcamp-1"
database_version      = "POSTGRES_12"
instance_tier         = "db-f1-micro"
disk_space            = 10
database_name         = "dbname"
project_database_name = "projectdbname"
db_username           = "dbuser"
db_password           = "dbpassword"

#BigQuery
dataset_id = "deb_capstone_dw"



sql_files = {
  "create_dim_device_table.sql" = "loading_data/sql_queries/create_dim_device_table.sql"
  "create_dim_location.sql"     = "loading_data/sql_queries/create_dim_location.sql"
  "create_dim_os.sql"           = "loading_data/sql_queries/create_dim_os.sql"
  "create_fact_table.sql"       = "loading_data/sql_queries/create_fact_table.sql"
  "create_table_dim_date.sql"   = "loading_data/sql_queries/create_table_dim_date.sql"
  "create_user_purchase_table.sql"  = "loading_data/sql_queries/create_user_purchase_table.sql"
  "import_user_purchase.sql"        = "loading_data/sql_queries/import_user_purchase.sql"
}