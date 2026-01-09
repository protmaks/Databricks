variable "environment" {
  type        = string
  description = "Environment name: dev, prod"
}

variable "dataset_catalog" {
  type        = string
  description = "Databricks catalog name"
}

variable "dataset_schema" {
  type        = string
  description = "Databricks schema name"
}

variable "warehouse_id" {
  type        = string
  description = "SQL Warehouse ID"
}

variable "parent_path" {
  type        = string
  description = "Parent folder path for dashboards"
}

variable "workspace_url" {
  type        = string
  description = "Databricks workspace URL"
}

variable "databricks_token" {
  type        = string
  sensitive   = true
  description = "Databricks authentication token"
}