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
