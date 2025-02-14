import requests
import time
import logging
from airflow.models import BaseOperator

class PowerBIOperator(BaseOperator):
    template_fields = (
        'tenant_id',
        'client_id',
        'client_secret',
        'group_id',
        'dataset_id',
    )

    def __init__(self,
                 tenant_id: str,
                 client_id: str,
                 client_secret: str,
                 group_id: str,
                 dataset_id: str,
                 timeout: int = 3600,
                 **kwargs) -> None:
        super().__init__(**kwargs)
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.group_id = group_id
        self.dataset_id = dataset_id
        self.timeout = timeout

    def execute(self, context):
        self.render_template_fields(context)

        # Authenticate with Azure AD
        token = self._get_access_token()

        # Trigger dataset refresh
        self._trigger_refresh(token)

        # Wait for the refresh to complete
        self._wait_for_refresh_completion(token)

    def _get_access_token(self):
        """Authenticate with Azure AD and return the access token."""
        url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
        data = {
            'grant_type': 'client_credentials',
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'scope': 'https://analysis.windows.net/powerbi/api/.default'
        }
        
        response = requests.post(url, data=data)
        response.raise_for_status()
        return response.json().get('access_token')

    def _trigger_refresh(self, token):
        """Trigger the Power BI dataset refresh."""
        url = f"https://api.powerbi.com/v1.0/myorg/groups/{self.group_id}/datasets/{self.dataset_id}/refreshes"
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }
        payload = {}

        response = requests.post(url, headers=headers, json=payload)
        if response.status_code not in [200, 202]:
            raise Exception(f"Failed to trigger refresh: {response.content}")

        logging.info(f"Refresh triggered successfully for dataset {self.dataset_id} in group {self.group_id}.")

    def _wait_for_refresh_completion(self, token):
        """Poll the API to check if the dataset refresh has completed."""
        url = f"https://api.powerbi.com/v1.0/myorg/groups/{self.group_id}/datasets/{self.dataset_id}/refreshes"
        headers = {
            'Authorization': f'Bearer {token}'
        }

        start_time = time.time()
        while time.time() - start_time < self.timeout:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            refreshes = response.json().get('value', [])

            if not refreshes:
                raise Exception("No refreshes found for the dataset.")

            last_refresh = refreshes[0]
            status = last_refresh.get('status')

            if status == 'Completed':
                logging.info("Dataset refresh completed successfully.")
                return
            elif status == 'Failed':
                raise Exception("Dataset refresh failed.")

            logging.info("Waiting for refresh to complete...")
            time.sleep(30)  # Wait 30 seconds before polling again

        raise Exception("Dataset refresh did not complete within the timeout period.")