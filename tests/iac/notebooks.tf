variable "notebook_subdirectory" {
  description = "A name for the subdirectory to store the notebook."
  type        = string
  default     = "lakefed_bulk_ingest"
}

resource "databricks_notebook" "generate_src_data_notebook" {
  path     = "${data.databricks_current_user.me.home}/${var.notebook_subdirectory}/generate_src_data_notebook"
  language = "PYTHON"
  source   = "./generate_src_data.py"
}

resource "databricks_notebook" "write_to_external_db_notebook" {
  path     = "${data.databricks_current_user.me.home}/${var.notebook_subdirectory}/write_to_external_db"
  language = "PYTHON"
  source   = "./write_to_external_db.py"
}