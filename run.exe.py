# Databricks notebook source
# MAGIC %md
# MAGIC ## Required input parameters
# MAGIC * app_id           ||   Service Principal application ID
# MAGIC * display_name     ||   Databricks will use this display name in the workspace
# MAGIC * catalog_name     ||   Name of the Databricks workspace catalog which will be used as the main catalog for Ikidata's automation solution
# MAGIC * scope_name       ||   Name of the Key Vault scope Ikidata's automation solution will be using. Service Principal requires read access there.
# MAGIC * server_hostname  ||   Databricks workspace hostname 'https://adb-1234556.1.azuredatabricks.net
# MAGIC * token            ||   Can be PAT or Entra ID token as long as token owner has admin rights

# COMMAND ----------

# DBTITLE 1,Importing modules and activating logger
from modules import AccessManagement, activate_logger, UnitTest
import os

# COMMAND ----------

# DBTITLE 1,Populate parameters
app_id = os.getenv('app_id')  
display_name = os.getenv('display_name')  
catalog_name = os.getenv('catalog_name')  
scope_name = os.getenv('scope_name')  
server_hostname = os.getenv('server_hostname')  
token = os.getenv('token')  

# COMMAND ----------

# DBTITLE 1,Validating input parameters
### Activating logger
logger = activate_logger()

### Calling unit tests
test = UnitTest(app_id, display_name, catalog_name, scope_name, server_hostname, token, logger)
test.validate_inputs()
test.validate_azure_app_id()
test.validate_catalog_name()
test.validate_databricks_url()

# COMMAND ----------

# DBTITLE 1,Granting mandatory access rights to the chosen Service Principal
action = 'create'
create = AccessManagement(app_id, display_name, catalog_name, scope_name, server_hostname, token, action, logger)
create.service_principal_management()
create.workspace_management()
create.table_management()
create.catalog_management()
create.key_vault_management()

# COMMAND ----------

# DBTITLE 1,Removing access rights from Service Principal
action = 'delete'
delete = AccessManagement(app_id, display_name, catalog_name, scope_name, server_hostname, token, action, logger)
delete.service_principal_management()
delete.workspace_management()
delete.table_management()
delete.catalog_management()
delete.key_vault_management()
