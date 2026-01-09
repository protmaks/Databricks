terraform {
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = ">= 1.100.0"
    }
  }
}

provider "databricks" {
  host  = var.workspace_url
  token = var.databricks_token
}

resource "databricks_dashboard" "sales" {
  display_name         = "NEW Terraform Dashboard - ${upper(var.environment)}"
  parent_path          = var.parent_path
  warehouse_id         = var.warehouse_id
  serialized_dashboard = file("${path.module}/test2.lvdash.json")
  
  dataset_catalog = var.dataset_catalog    # 🔥 NEW
  dataset_schema  = var.dataset_schema     # 🔥 NEW
}

output "dashboard_url" {
  value = "https://${var.workspace_url}/sql/dashboardsv3/${databricks_dashboard.sales.id}"
}

output "environment" {
  value = var.environment
}