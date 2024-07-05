# service_principal_management
This is a repository for automated permission management for service principals.

[![License](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

This Python project provides a simple and efficient way to create Databricks Service Principals to Databricks workspace and grant mandatory permissions for Ikidata's automated solution.

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
* Key Vault where Webhook destination url will be stored.
* Service Principal requires admin rights to the workspace or otherwise it can't monitor everything - it will be added to the "admins" user group automatically.

## Usage

Repository contains example run.exe notebook which has easy-to-use instructions and automated unit testing. Parameters can be passed as an environmental variables. The notebook has one main class called 'AccessManagement' contains five modules, which are explained down below:

| Module                       | Description                                               |      Actions        |
|------------------------------|-----------------------------------------------------------|---------------------|
| service_principal_management | Creating a new Service Principal to Databricks workspace  |    create/delete    |
| workspace_management         | Creating "/ikidata" master folder and granting permissions|    create/delete    |  
| table_management             | Granting use and read permissions to the required tables  |    create/delete    |
| catalog_management           | Granting all privileges permissions to the chosen catalog |    create/delete    |
| key_vault_management         | Granting read permission on the chosen Key Vault scope    |    create/delete    |

## Examples

```python
# Importing modules
from modules import AccessManagement

#app_id = '123456789'
display_name = 'ikidata_test_sp'
catalog_name = 'ikidata_catalog'
scope_name = 'kv-customer'
server_hostname = 'https://adb-123456789.1.azuredatabricks.net'
token = dbutils.secrets.get(scope = 'scope', key = 'admin-PAT') 
action = 'create'

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
```
## Catalog, Schema & Table access rights
The user will be granted 'ALL_PRIVILEGES' access rights to the selected catalog.

The following catalog will be granted 'USE_CATALOG' access rights:
* system

The following schemas will be granted 'USE_SCHEMA' access rights:
* system.access, system.billing, system.workflow

The following tables will be granted 'SELECT' access rights:
* system.access.audit, system.billing.list_prices, system.billing.usage, system.workflow.jobs
