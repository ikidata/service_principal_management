# Databricks notebook source
# MAGIC %md
# MAGIC ## Required input parameters
# MAGIC * display_name     ||   Databricks will use this display name in the workspace for the Service Principal
# MAGIC * catalog_name     ||   Name of the Databricks workspace catalog which will be used as the main catalog for Ikidata's automation solution
# MAGIC * scope_name       ||   Name of the Key Vault scope Ikidata's automation solution will be using. Service Principal requires read access there.
# MAGIC * server_hostname  ||   Databricks workspace hostname 'https://adb-1234556.1.azuredatabricks.net
# MAGIC * token            ||   Can be PAT or Entra ID token as long as token owner has admin rights
# MAGIC * action           ||   Can be 'create' or 'delete' only. When 'delete' is used, app_id is required parameter.
# MAGIC * app_id           ||   Azure Service Principal application ID. Optional parameter and on default not activated (using Databricks Service Principals is recommended). When deleting access rights & Service Principal, app_id parameter is required.

# COMMAND ----------

# DBTITLE 1,Importing modules and activating logger
from modules import AccessManagement
import os

# COMMAND ----------

# DBTITLE 1,Populate parameters
### Required
display_name = os.getenv('display_name')  
catalog_name = os.getenv('catalog_name')  
scope_name = os.getenv('scope_name')  
server_hostname = os.getenv('server_hostname')  
token = os.getenv('token')  
action = os.getenv('action')  ### Can be 'create' or 'delete' only

### Optional
app_id = os.getenv('app_id')  ### Optional. If Entra ID Service Principal needs to be used instead of Databricks Service Principal, app id can be populated here. Otherwise leave it empty.

# COMMAND ----------

# DBTITLE 1,Running the code
# When deleting Service Principal & access rights, app_id parameter is required. 
main = AccessManagement(display_name = display_name, 
                        catalog_name = catalog_name, 
                        scope_name = scope_name, 
                        server_hostname = server_hostname, 
                        token = token, 
                        action = action,
                        app_id = '')  # when action = 'delete', app_id parameter is required


#Granting / removing mandatory access rights to the chosen Service Principal
main.service_principal_management()
main.workspace_management()
main.table_management()
main.catalog_management()
main.key_vault_management()
