import json
import requests
import logging
#from modules import activate_logger, UnitTest
from modules.logger import activate_logger
from modules.utils import UnitTest
import pandas as pd

class AccessManagement():
    def __init__(self, display_name: str, catalog_name: str, scope_name: str, server_hostname: str, token: str, action: str, app_id: str = '', logger: str = ''):
        self.app_id = app_id
        self.display_name = display_name
        self.catalog_name = catalog_name
        self.scope_name = scope_name
        self.server_hostname = server_hostname
        self.token = token
        self.action = action

        ### Activating logger if it's not passed as a parameter
        if logger != '':
            self.logger = logger
        else:
            self.logger = activate_logger() 
        
        ### Running unit tests
        self.run_tests()

    
    def run_tests(self) -> None:  
            '''
            Calling all unit tests
            '''
            self.test = UnitTest(self.app_id, self.display_name, self.catalog_name, self.scope_name, self.server_hostname, self.token, self.action, self.logger)
            self.test.validate_inputs()
            self.test.validate_action()
            if self.app_id and self.app_id != '':  
                self.test.validate_azure_app_id()
            self.test.validate_catalog_name()
            self.test.validate_databricks_url()
            self.test.validating_existing_service_principals()
            self.logger.info("All tests have been executed.")  
    
    def fetching_admin_group_id(self) -> str:
        '''
        The function fetches admin group ID for the chosen workspace.
        '''
        api_version = '/api/2.0'
        api_command = '/preview/scim/v2/Groups'  
        url = f"{self.server_hostname}{api_version}{api_command}"
        headers = {'Authorization': 'Bearer %s' % self.token}

        session = requests.Session()
        resp = session.request('GET', url, verify = True, headers=headers) 
        groups = resp.json().get('Resources', [])  
        admin_group_id = None  
        for group in groups:  
            if group.get('displayName') == 'admins':  
                admin_group_id = group.get('id')  
                return admin_group_id 
        if not admin_group_id:  
            raise Exception('Admin group not found')  

        
    def service_principal_management(self) -> None:
        '''
        Input parameters:
        token: str
        Databricks Service Principal is used for authentication. After the Databrick Service Principal is created, secret needs to be created manually from Databricks UI.

        server_hostname str:
        Databricks server hostname in the next format:
        https://adb-123456789.12.azuredatabricks.net'

        display_name: str
        Display name for Databricks Service Principal in Databricks workspace. It can't be empty.

        action: str
        It can be "create" or "delete".
        '''

        if self.action.lower() == 'create':

            ### Fetching Admin Group ID
            admin_group_id = self.fetching_admin_group_id()

            api_version = '/api/2.0'
            api_command = '/preview/scim/v2/ServicePrincipals'
            url = f"{self.server_hostname}{api_version}{api_command}"
            headers = {'Authorization': 'Bearer %s' % self.token}

            ### Using correct payload based in Service Principal type
            if self.app_id is None or self.app_id == '':  
                payload = {'displayName': self.display_name,              
                'groups': [{'value': admin_group_id}],  # Adding to "admins" group 
                'entitlements': [{'value': 'workspace-access'},
                    {'value': 'databricks-sql-access'},
                    {'value': 'allow-cluster-create'}],
                'active': True}
            else:
                payload = {'displayName': self.display_name,              
                'groups': [{'value': admin_group_id}],
                'entitlements': [{'value': 'workspace-access'},
                    {'value': 'databricks-sql-access'},
                    {'value': 'allow-cluster-create'}],
                'applicationId': self.app_id, 
                'active': True}


            session = requests.Session()
            resp = session.request('POST', url, data=json.dumps(payload), verify = True, headers=headers) 
            assert (resp.status_code == 201) | (resp.status_code == 409), f"Creating Service Principal {self.display_name} has failed. Reason: {resp.status_code} {resp.json()}"
            if resp.status_code == 409:
                self.logger.info(f"Service Principal {self.display_name} already exists.")
            else:
                self.logger.info(f"Creating Service Principal {self.display_name} has succeeded.")
                self.app_id = resp.json()['applicationId']
    
        elif self.action.lower() == 'delete':
            assert (self.app_id != None and self.app_id != ''), 'Service Principal App ID was empty. Please check it again.'
            api_version = '/api/2.0'
            api_command = '/preview/scim/v2/ServicePrincipals'
            url = f"{self.server_hostname}{api_version}{api_command}"
            headers = {'Authorization': 'Bearer %s' % self.token}
            session = requests.Session()
            resp = session.request('GET', url, verify = True, headers=headers) 

            for sp in resp.json()['Resources']:
                if sp['applicationId'] == self.app_id:
                    sp_id = sp['id']
                    sp_display_name = sp['displayName']
                    break
            

            api_command = f'/preview/scim/v2/ServicePrincipals/{sp_id}'
            url = f"{self.server_hostname}{api_version}{api_command}"
            headers = {'Authorization': 'Bearer %s' % self.token}

            session = requests.Session()
            resp = session.request('DELETE', url, verify = True, headers=headers) 
            assert resp.status_code == 204, f"Deleting Service Principal {sp_display_name} with Application ID {self.app_id} has failed. Reason: {resp.json()}"
            self.logger.info(f"Deleting Service Principal {sp_display_name} with Application ID {self.app_id} has succeeded.")
        
        else:
            self.logger.warning(f"Wrong action input parameter. It can be 'create' or 'delete' and you used {self.action}")

    def workspace_management(self) -> None:
        '''
        Input parameters:
        token: str
        Authentication can be done with Databricks PAT (personal access token) or Microsoft Entra ID token.

        server_hostname str:
        Databricks server hostname in the next format:
        https://adb-123456789.12.azuredatabricks.net'

        app_id: str
        Service Principal's Application ID. It can't be empty.

        action: str
        It can be "create" or "delete".
        '''
    
        if self.action.lower() == 'create':
            api_version = '/api/2.0' 
            api_command = '/workspace/mkdirs'
            url = f"{self.server_hostname}{api_version}{api_command}"
            headers = {'Authorization': 'Bearer %s' % self.token}
            session = requests.Session()
            payload = {"path": "/Ikidata"}
            resp = session.request('POST', url, data=json.dumps(payload), verify = True, headers=headers)
            assert resp.status_code == 200, f"Creating path '/Ikidata' has failed. Reason: {resp.json()['message']}"
            self.logger.info(f"Path '/Ikidata' has been created")

            api_command = '/workspace/list'
            url = f"{self.server_hostname}{api_version}{api_command}"
            headers = {'Authorization': 'Bearer %s' % self.token}
            session = requests.Session()
            payload = {"path": "/Workspace"}
            resp = session.request('GET', url, data=json.dumps(payload), verify = True, headers=headers)
            assert resp.status_code == 200, f"Fetching workspace object ID has failed. Reason: {resp.json()['message']}"
        
            for object in resp.json()['objects']:
                if object['path'] == '/Workspace/Ikidata':
                    object_id = object['object_id']
                    self.logger.info(f"Object ID for '/Workspace/Ikidata' master folder has been found and it's {object_id}")
                    break

            api_command = f'/permissions/directories/{object_id}'
            url = f"{self.server_hostname}{api_version}{api_command}"
            headers = {'Authorization': 'Bearer %s' % self.token}
            session = requests.Session()

            payload = {
            "access_control_list": [
                {
                "service_principal_name": self.app_id,
                "permission_level": "CAN_MANAGE"
                }
            ]
            }
            resp = session.request('PUT', url, data=json.dumps(payload), verify = True, headers=headers)
            assert resp.status_code == 200, f"Adding 'CAN MANAGE' permissions to '/Workspace/Ikidata' object has failed. Reason: {resp.json()['message']}"
            self.logger.info(f"'CAN MANAGE' permissions has been added to {self.app_id} successfully.")

        elif self.action.lower() == 'delete':
            api_version = '/api/2.0' 
            api_command = '/workspace/delete'
            url = f"{self.server_hostname}{api_version}{api_command}"
            headers = {'Authorization': 'Bearer %s' % self.token}
            session = requests.Session()
            payload = {"path": "/Ikidata",
                    "recursive": "true"}
            resp = session.request('POST', url, data=json.dumps(payload), verify = True, headers=headers)
            assert resp.status_code == 200, f"Deleting path '/Ikidata' has failed. Reason: {resp.json()['message']}"
            self.logger.info(f"Path '/Ikidata' has been deleted")
        else:
            self.logger.warning(f"Wrong action input parameter. It can be 'create' or 'delete' and you used {self.action}")


    def table_management(self) -> None:
        '''
        Input parameters:
        token: str
        Authentication can be done with Databricks PAT (personal access token) or Microsoft Entra ID token.

        server_hostname str:
        Databricks server hostname in the next format:
        https://adb-123456789.12.azuredatabricks.net'

        app_id: str
        Service Principal's Application ID. It can't be empty.

        action: str
        It can be "create" or "delete".
        '''
        
        if self.action.lower() == 'create':

            securable_type = 'catalog'
            catalog_name = 'system'

            api_version = '/api/2.1'
            api_command = f'/unity-catalog/permissions/{securable_type}/{catalog_name}'
            url = f"{self.server_hostname}{api_version}{api_command}"
            headers = {'Authorization': 'Bearer %s' % self.token}
            session = requests.Session()
            payload = {
            "changes": [
                {
                "principal": self.app_id,
                "add": [
                    "USE_CATALOG"
                ]}]}

            resp = session.request('PATCH', url, data=json.dumps(payload), verify = True, headers=headers) 
            assert resp.status_code == 200, f"Granting USE CATALOG permission on {catalog_name} to Application ID {self.app_id} has failed. Reason: {resp.json()}"
            self.logger.info(f"Granting USE CATALOG permission on {catalog_name} to Application ID {self.app_id} has succeeded")

            schema_list = ['system.access', 'system.billing', 'system.compute', 'system.information_schema', 'system.workflow']
            securable_type = 'schema'

            for schema_name in schema_list:
                api_command = f'/unity-catalog/permissions/{securable_type}/{schema_name}'
                url = f"{self.server_hostname}{api_version}{api_command}"
                headers = {'Authorization': 'Bearer %s' % self.token}
                session = requests.Session()
                payload = {
                "changes": [
                    {
                    "principal": self.app_id,
                    "add": [
                        "USE_SCHEMA"
                    ]}]}

                resp = session.request('PATCH', url, data=json.dumps(payload), verify = True, headers=headers) 
                assert resp.status_code == 200, f"Granting USE SCHEMA permission on {schema_name} to Application ID {self.app_id} has failed. Reason: {resp.json()}"
                self.logger.info(f"Granting USE SCHEMA permission on {schema_name} to Application ID {self.app_id} has succeeded")

            tables_list = ['system.access.audit', 'system.billing.list_prices', 'system.billing.usage', 'system.compute.clusters', 'system.information_schema.table_privileges', 'system.information_schema.schema_privileges', 'system.information_schema.catalog_privileges', 'system.information_schema.volume_privileges', 'system.workflow.jobs']
            securable_type = 'table'

            for table_name in tables_list:

                api_version = '/api/2.1'
                api_command = f'/unity-catalog/permissions/{securable_type}/{table_name}'
                url = f"{self.server_hostname}{api_version}{api_command}"
                headers = {'Authorization': 'Bearer %s' % self.token}
                session = requests.Session()
                payload = {
            "changes": [
                {
                "principal": self.app_id,
                "add": [
                    "SELECT"
                ]}]}

                resp = session.request('PATCH', url, data=json.dumps(payload), verify = True, headers=headers) 
                assert resp.status_code == 200, f"Granting SELECT permission on {table_name} to Application ID {self.app_id} has failed. Reason: {resp.json()}"
                self.logger.info(f"Granting USE SELECT permission on {table_name} to Application ID {self.app_id} has succeeded")

        elif self.action.lower() == 'delete':

            securable_type = 'catalog'
            catalog_name = 'system'

            api_version = '/api/2.1'
            api_command = f'/unity-catalog/permissions/{securable_type}/{catalog_name}'
            url = f"{self.server_hostname}{api_version}{api_command}"
            headers = {'Authorization': 'Bearer %s' % self.token}
            session = requests.Session()
            payload = {
            "changes": [
                {
                "principal": self.app_id,
                "remove": [
                    "USE_CATALOG"
                ]}]}

            resp = session.request('PATCH', url, data=json.dumps(payload), verify = True, headers=headers) 
            assert resp.status_code == 200, f"Removing USE CATALOG permission on {catalog_name} to Application ID {self.app_id} has failed. Reason: {resp.json()}"
            self.logger.info(f"Removing USE CATALOG permission on {catalog_name} to Application ID {self.app_id} has succeeded")

            schema_list = ['system.access', 'system.billing', 'system.information_schema', 'system.workflow']
            securable_type = 'schema'

            for schema_name in schema_list:
                api_command = f'/unity-catalog/permissions/{securable_type}/{schema_name}'
                url = f"{self.server_hostname}{api_version}{api_command}"
                headers = {'Authorization': 'Bearer %s' % self.token}
                session = requests.Session()
                payload = {
                "changes": [
                    {
                    "principal": self.app_id,
                    "remove": [
                        "USE_SCHEMA"
                    ]}]}

                resp = session.request('PATCH', url, data=json.dumps(payload), verify = True, headers=headers) 
                assert resp.status_code == 200, f"Removing USE SCHEMA permission on {schema_name} to Application ID {self.app_id} has failed. Reason: {resp.json()}"
                self.logger.info(f"Removing USE SCHEMA permission on {schema_name} to Application ID {self.app_id} has succeeded")


            tables_list = ['system.access.audit', 'system.billing.list_prices', 'system.billing.usage', 'system.information_schema.table_privileges', 'system.information_schema.schema_privileges', 'system.information_schema.catalog_privileges', 'system.information_schema.volume_privileges', 'system.workflow.jobs']
            securable_type = 'table'

            for table_name in tables_list:

                api_version = '/api/2.1'
                api_command = f'/unity-catalog/permissions/{securable_type}/{table_name}'
                url = f"{self.server_hostname}{api_version}{api_command}"
                headers = {'Authorization': 'Bearer %s' % self.token}
                session = requests.Session()
                payload = {
            "changes": [
                {
                "principal": self.app_id,
                "remove": [
                    "SELECT"
                ]}]}

                resp = session.request('PATCH', url, data=json.dumps(payload), verify = True, headers=headers) 
                assert resp.status_code == 200, f"Removing SELECT permission on {table_name} to Application ID {self.app_id} has failed. Reason: {resp.json()}"
                self.logger.info(f"Removing USE SELECT permission on {table_name} to Application ID {self.app_id} has succeeded")

        else:
            self.logger.warning(f"Wrong action input parameter. It can be 'create' or 'delete' and you used {self.action}")

    def catalog_management(self) -> None:
        '''
        Input parameters:
        token: str
        Authentication can be done with Databricks PAT (personal access token) or Microsoft Entra ID token.

        server_hostname str:
        Databricks server hostname in the next format:
        https://adb-123456789.12.azuredatabricks.net'

        app_id: str
        Service Principal's Application ID. It can't be empty.

        catalog_name: str
        The main catalog which will be used for Ikidata's automation solution. 

        action: str
        It can be "create" or "delete".
        '''
    
        if self.action.lower() == 'create':

            securable_type = 'catalog'
            api_version = '/api/2.1'
            api_command = f'/unity-catalog/permissions/{securable_type}/{self.catalog_name}'
            url = f"{self.server_hostname}{api_version}{api_command}"
            headers = {'Authorization': 'Bearer %s' % self.token}
            session = requests.Session()
            payload = {
            "changes": [
                {
                "principal": self.app_id,
                "add": [ 
                    "ALL_PRIVILEGES"
                ]}]}

            resp = session.request('PATCH', url, data=json.dumps(payload), verify = True, headers=headers) 
            assert resp.status_code == 200, f"Granting ALL PRIVILEGES permission on {self.catalog_name} to Application ID {self.app_id} has failed. Reason: {resp.json()}"
            self.logger.info(f"Granting ALL PRIVILEGES permission on {self.catalog_name} to Application ID {self.app_id} has succeeded")

        elif self.action.lower() == 'delete':

            securable_type = 'catalog'
            api_version = '/api/2.1'
            api_command = f'/unity-catalog/permissions/{securable_type}/{self.catalog_name}'
            url = f"{self.server_hostname}{api_version}{api_command}"
            headers = {'Authorization': 'Bearer %s' % self.token}
            session = requests.Session()
            payload = {
            "changes": [
                {
                "principal": self.app_id,
                "remove": [ 
                    "ALL_PRIVILEGES"
                ]}]}

            resp = session.request('PATCH', url, data=json.dumps(payload), verify = True, headers=headers) 
            assert resp.status_code == 200, f"Removing ALL PRIVILEGES permission on {self.catalog_name} to Application ID {self.app_id} has failed. Reason: {resp.json()}"
            self.logger.info(f"Removing ALL PRIVILEGES permission on {self.catalog_name} to Application ID {self.app_id} has succeeded")

        else:
            self.logger.warning(f"Wrong action input parameter. It can be 'create' or 'delete' and you used {self.action}")

    def key_vault_management(self) -> None:
        '''
        Input parameters:
        token: str
        Authentication can be done with Databricks PAT (personal access token) or Microsoft Entra ID token.

        server_hostname str:
        Databricks server hostname in the next format:
        https://adb-123456789.12.azuredatabricks.net'

        app_id: str
        Service Principal's Application ID. It can't be empty.

        scope_name: str
        The scope name of the Key Vault in Databricks workspace. 

        action: str
        It can be "create" or "delete".
        '''
    
        if self.action.lower() == 'create':

            api_version = '/api/2.0'
            api_command = '/secrets/acls/put'
            url = f"{self.server_hostname}{api_version}{api_command}"
            headers = {'Authorization': 'Bearer %s' % self.token}
            session = requests.Session()
            payload = {
                        "scope": self.scope_name,
                        "principal": self.app_id,
                        "permission": "READ"
                        }

            resp = session.request('POST', url, data=json.dumps(payload), verify = True, headers=headers) 
            assert resp.status_code == 200, f"Granting READ permission on scope {self.scope_name} to Application ID {self.app_id} has failed. Reason: {resp.json()}"
            self.logger.info(f"Granting READ permission on scope {self.scope_name} to Application ID {self.app_id} has succeeded")

        elif self.action.lower() == 'delete':

            api_version = '/api/2.0'
            api_command = '/secrets/acls/delete'
            url = f"{self.server_hostname}{api_version}{api_command}"
            headers = {'Authorization': 'Bearer %s' % self.token}
            session = requests.Session()
            payload = {
                    "scope": self.scope_name,
                    "principal": self.app_id
                        }

            resp = session.request('POST', url, data=json.dumps(payload), verify = True, headers=headers) 
            assert resp.status_code == 200, f"Removing READ permission on scope {self.scope_name} to Application ID {self.app_id} has failed. Reason: {resp.json()}"
            self.logger.info(f"Removing READ permission on scope {self.scope_name} to Application ID {self.app_id} has succeeded")

        else:
            self.logger.warning(f"Wrong action input parameter. It can be 'create' or 'delete' and you used {self.action}")