output "resource_group_name" {
  value = azurerm_resource_group.default.name
}

output "azurerm_postgresql_flexible_server" {
  value = azurerm_postgresql_flexible_server.default.name
}

output "postgresql_flexible_server_admin_password" {
  sensitive = true
  value     = azurerm_postgresql_flexible_server.default.administrator_password
}

output "databricks_secret_scope_name" {
 value = databricks_secret_scope.this.name
}

output "databricks_job_url" {
  value = databricks_job.this.url
}

output "generate_src_data_notebook_url" {
 value = databricks_notebook.generate_src_data_notebook.url
}

output "write_to_external_db_notebook" {
 value = databricks_notebook.write_to_external_db_notebook.url
}