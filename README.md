# service_principal_management
This is a repository for automated permission management for service principals.

[![License](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

This Python project provides a simple and efficient way to import Azure Service Principals to Databricks workspace and grant mandatory permissions for Ikidata's automated solution.

## Table of Contents

- [Installation](#installation)
- [Prerequisites](#prerequisites)
- [Usage](#usage)
- [Examples](#examples)

## Installation

To use this Python project, you need to have Python installed on your system. Simply clone this repository and import all classes from `module` into your Python project.

python
pip install git+https://github.com/ikidata/service_principal_management

## Prerequisites
* Databricks cluster DBR Version: 13.3 LTS 
* Required python libraries can be found from requirements.txt file. 
* Requires working token (personal access token / Entra ID Token) which owner has workspace admin rights and manage permissions on Unity Catalog to be able to provide access rights to the Service Principal. 
* Granting access to main catalog which will be used for Ikidata's automated solution.
* Key Vault where Service Principal secret, ID and Azure Tenant ID will be stored. Remember that the chosen Service Principal needs to have access to the Key Vault (ie. Key Vault Secrets Officer role).
* Service Principal requires admin rights to the workspace or otherwise it can't monitor everything. Remember to Service Principal manually to "admins" user group.

## Usage

Repository contains example run.exe notebook which has easy-to-use instructions. the 'Modules' library contains five classes which can be found down below:

| Class                        | Description                                               |      Actions        |
|------------------------------|-----------------------------------------------------------|---------------------|
| service_principal_management | Adding new Service Principal to Databricks workspace      |    create/delete    |
| workspace_management         | Creating "/ikidata" master folder and granting permissions|    create/delete    |  
| table_management             | Granting use and read permissions to the required tables  |    create/delete    |
| catalog_management           | Granting all privileges permissions to the chosen catalog |    create/delete    |
| key_vault_management         | Granting read permission on the chosen Key Vault scope    |    create/delete    |

## Examples

```python
# Importing modules
from modules import service_principal_management, workspace_management, table_management, catalog_management, key_vault_management

app_id = '123456789'
display_name = 'ikidata_test_sp'
catalog_name = 'ikidata_catalog'
server_hostname = 'https://adb-123456789.1.azuredatabricks.net'
token = dbutils.secrets.get(scope = 'scope', key = 'admin-PAT') 

# Granting permissions
service_principal_management(token, server_hostname, app_id, display_name, 'create')
workspace_management(token, server_hostname, app_id, 'create')
table_management(token, server_hostname, app_id, 'create')
catalog_management(token, server_hostname, app_id, catalog_name, 'create')
key_vault_management(token, server_hostname, app_id, scope_name, 'create')

# Removing permissions
table_management(token, server_hostname, app_id, 'delete')
workspace_management(token, server_hostname, app_id, 'delete')
service_principal_management(token, server_hostname, app_id, display_name, 'delete')
catalog_management(token, server_hostname, app_id, catalog_name, 'delete')
service_principal_management(token, server_hostname, app_id, display_name, 'delete')
```