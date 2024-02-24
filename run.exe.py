# Databricks notebook source
# MAGIC %md
# MAGIC ## Required input parameters
# MAGIC * app_id  ||  Service Principal application ID
# MAGIC * display_name  ||  Databricks will use this display name in the workspace
# MAGIC * catalog_name  ||  Name of the Databricks workspace catalog which will be used as the main catalog for Ikidata's automation solution
# MAGIC * server_hostname  ||  Databricks workspace hostname 'https://adb-1234556.1.azuredatabricks.net
# MAGIC * token   ||   Can be PAT or Entra ID token as long as token owner has admin rights

# COMMAND ----------

# DBTITLE 1,Populate parameters
app_id = ''
display_name = ''
catalog_name = ''
server_hostname = ''
token = ''  # Using Azure Key Vault HIGHLY RECOMMENDED: dbutils.secrets.get(scope = '', key = '') 

# COMMAND ----------

# DBTITLE 1,Importing modules
from modules import service_principal_management, workspace_management, table_management, catalog_management

# COMMAND ----------

# DBTITLE 1,Granting mandatory access rights to the chosen Service Principal
service_principal_management(token, server_hostname, app_id, display_name, 'create')
workspace_management(token, server_hostname, app_id, 'create')
table_management(token, server_hostname, app_id, 'create')
catalog_management(token, server_hostname, app_id, catalog_name, 'create')

# COMMAND ----------

# DBTITLE 1,Removing access rights from Service Principal
catalog_management(token, server_hostname, app_id, catalog_name, 'delete')
table_management(token, server_hostname, app_id, 'delete')
workspace_management(token, server_hostname, app_id, 'delete')
service_principal_management(token, server_hostname, app_id, display_name, 'delete')
