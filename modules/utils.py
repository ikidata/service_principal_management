import pandas as pd
import re
from modules.logger import activate_logger
import logging
import requests
import pandas as pd

class UnitTest():
    def __init__(self, app_id: str, display_name: str, catalog_name: str, scope_name: str, server_hostname: str, token: str, action: str, logger: str = ''):
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

    def validate_inputs(self) -> None:  
        '''  
        Validates the input parameters to ensure each one is a non-empty string.   
        Raises ValueError if any input is not a string or if it is an empty string.  
        '''  
        inputs = [self.display_name, self.catalog_name, self.scope_name, self.server_hostname, self.token]  
    
        for input in inputs:  
            if not isinstance(input, str):  
                raise ValueError(f"Expected string but got {type(input).__name__}")  
            elif not input:  
                raise ValueError(f"String cannot be empty for {input}")  
        
        self.logger.info(f"Validate inputs unit test has been passed")

    def validate_action(self) -> None:  
        '''  
        Validates 'action' input parameter. It can be 'create' or 'delete' only.
        '''  
        valid_actions = ['create', 'delete']  
        if self.action not in valid_actions:  
            raise ValueError(f"Invalid action: {self.action}. Allowed values are 'create' or 'delete'.")  

        self.logger.info(f"Validate 'action' input parameter unit test has been passed")

    def validate_azure_app_id(self) -> None:  
        '''
        Validates the Azure AD application ID to ensure it is a non-empty string and matches the GUID format.  
        Raises ValueError if the input is not a string or if it doesn't match the GUID format.  
        Azure AD application ID is a 32-character long GUID and Format: XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX  
        '''
        self.logger.info(f"Azure Service Principal was chosen instead of Databricks Service Principal")

        guid_regex = r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'  
    
        if not isinstance(self.app_id, str):  
            raise ValueError(f"Expected string but got {type(self.app_id).__name__}")  
        elif not re.fullmatch(guid_regex, self.app_id):  
            raise ValueError("Invalid Azure AD application ID format")  

        self.logger.info(f"Validate Azure app ID unit test has been passed")

  
    def validate_catalog_name(self) -> None:   
        '''  
        Validates the catalog_name to ensure it only contains alphanumeric characters, underscores, and hyphens.  
        Raises ValueError if the input is not a string or if it contains any other special characters.  
        '''  
        valid_chars_regex = r'^[a-zA-Z0-9_-]*$'    
    
        if not isinstance(self.catalog_name, str):    
            raise ValueError(f"Expected string but got {type(self.catalog_name).__name__}")   
            
        elif not re.fullmatch(valid_chars_regex, self.catalog_name):    
            raise ValueError("Invalid string format. String can only contain alphanumeric characters, underscores, and hyphens.")   

        self.logger.info(f"Validate Catalog name unit test has been passed")
  
    def validate_databricks_url(self) -> None:   
        '''  
        Validates the server_hostname input to ensure it starts with 'https://adb-' and ends with '.azuredatabricks.net'.  
        Raises ValueError if the server_hostname input is not a string or if it doesn't match the required format.  
        '''  
        url_regex = r'^https://adb-.*\.azuredatabricks\.net$'    
    
        if not isinstance(self.server_hostname, str):    
            raise ValueError(f"Expected string but got {type(self.server_hostname).__name__}")    
        elif not re.fullmatch(url_regex, self.server_hostname):    
            raise ValueError("Invalid URL format. URL must start with 'https://adb-' and end with '.azuredatabricks.net'.")
            
        self.logger.info(f"Validate Databricks server hostname unit test has been passed")

    def validating_existing_service_principals(self) -> None:
        '''
        When creating a Service Principal, the function validates that there won't be existing Service Principal with a same display name (Databricks allows it). When deleting, validating that Service Principal exists there.
        '''
        api_version = '/api/2.0'
        api_command = f'/preview/scim/v2/ServicePrincipals'
        url = f"{self.server_hostname}{api_version}{api_command}"
        headers = {'Authorization': 'Bearer %s' % self.token}

        session = requests.Session()
        resp = session.request('GET', url, verify = True, headers=headers) 

        ### Validate results
        df = pd.DataFrame(resp.json()['Resources'])

        if self.action == 'create':
            if len(df[df['displayName'] == self.display_name]) != 0:  
                raise ValueError(f"Service Principal with name {self.display_name} already exists. Please choose another name.")  
            else:
                self.logger.info(f"Existing Service Principal unit test has been passed")
        
        elif self.action == 'delete':
            if len(df[df['displayName'] == self.display_name]) == 0:  
                raise ValueError(f"Service Principal with name {self.display_name} doesn't exist. Please check your Display Name.")  
            else:
                self.logger.info(f"Existing Service Principal unit test has been passed")

        else:
            raise ValueError(f"Wrong 'action' parameter: {self.action}")