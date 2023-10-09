variable "project_id" {
  description = "project id"
}

variable "region" {
  description = "region"
}


# variable "sql_script_source" {
#   type = set(string)
#   description = "sql queries"
# }

# variable "sql_file_names" {
#   type = list(string)
#   description = "name of files as loaded"
# }

variable "sql_files" {
  type = map(string)
}