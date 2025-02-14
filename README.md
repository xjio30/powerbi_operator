# Power BI Operator for Apache Airflow

This repository provides a custom Apache Airflow operator, `PowerBIOperator`, designed to trigger and monitor dataset refreshes in Microsoft Power BI.

## Features

- Authenticate with Azure Active Directory using client credentials.
- Trigger a dataset refresh in Power BI.
- Monitor the refresh status until completion or timeout.

## Requirements

- Python 3.7+
- Apache Airflow 2.x
- `requests` library (ensure it's installed in your environment)

## Installation

1. Clone this repository or copy the `PowerBIOperator` class into your Airflow project.
2. Ensure the `requests` library is installed:
   ```bash
   pip install requests
   ```
3. Add the operator to your DAGs as needed.

## Usage

Here is an example of how to use the `PowerBIOperator` in an Airflow DAG following best practices:

```python
import pendulum
from airflow import DAG
from operators.powerbi_operator import PowerBIOperator

with DAG(
    dag_id='dag_example_pbi',
    schedule="0 1 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="Europe/Madrid"),
    catchup=False,
) as dag:

    pbi_operator = PowerBIOperator(
        dag=dag,
        task_id='refresh-pbi',
        trigger_rule='all_done',
        retries=2,
        tenant_id="{{ var.value.get('PBI_TENANT_ID') }}",
        client_id="{{ var.value.get('PBI_CLIENT_ID') }}",
        client_secret="{{ var.value.get('PBI_CLIENT_SECRET') }}",
        group_id="{{ var.value.get('PBI_GROUP_ID') }}",
        dataset_id="{{ var.value.get('PBI_DATASET_ID') }}")

    pbi_operator
```

### Parameters

- **tenant_id** *(str)*: Azure Active Directory tenant ID.
- **client_id** *(str)*: Client ID of the registered Azure AD application.
- **client_secret** *(str)*: Client secret of the Azure AD application.
- **group_id** *(str)*: Power BI workspace (group) ID.
- **dataset_id** *(str)*: Power BI dataset ID.
- **timeout** *(int)*: Maximum time (in seconds) to wait for the dataset refresh to complete (default: 3600 seconds).

## How It Works

1. **Authenticate with Azure AD**: The operator uses client credentials to authenticate with Azure Active Directory and retrieve an access token.
2. **Trigger Dataset Refresh**: It makes a POST request to the Power BI API to initiate a dataset refresh.
3. **Monitor Refresh Status**: It polls the Power BI API at regular intervals to check the status of the dataset refresh. The operator exits successfully if the refresh is completed or raises an exception if it fails or times out.

## Error Handling

- If the dataset refresh fails, an exception is raised with the error details.
- If no refreshes are found for the dataset, the operator raises an exception.
- If the refresh does not complete within the timeout period, an exception is raised.

## Contributing

Contributions are welcome! Feel free to open an issue or submit a pull request.

## License

This project is licensed under the [Apache 2.0 License](https://www.apache.org/licenses/LICENSE-2.0).

