import datetime

from airflow import DAG
from airflow.contrib.operators import bigquery_get_data
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.operators.bigquery_table_delete_operator import BigQueryTableDeleteOperator

from airflow.operators import bash_operator
from airflow.operators import email_operator
from airflow.utils import trigger_rule
from airflow.utils.dates import days_ago
from datetime import timedelta

gcs_bucket = 'bq-ga360-dumps'
output_file = 'gs://' + gcs_bucket + '/{{ ds_nodash }}.csv.gz'
source_table ='104737153.ga_sessions_{{ ds_nodash }}'
target_table ='104737153.ga_tmp_{{ ds_nodash }}'

# [START composer_notify_failure]
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

dag = DAG('ga360_etl',
        	schedule_interval=timedelta(days=1),
        	default_args=default_args)

extract_ga360 = BigQueryOperator(
	dag=dag,
    task_id='bq_unnest_yesterdays_table',
    bigquery_conn_id='zwift_ga360_bigquery',
    sql="""SELECT visitId, clientId FROM `{source_table}`""".format(source_table=source_table),
    use_legacy_sql=False,
    destination_dataset_table=target_table)

copy_ga360_to_gcs = BigQueryToCloudStorageOperator(
	dag=dag,
    task_id='export_recent_yesterday_to_gcs',
    bigquery_conn_id='zwift_ga360_bigquery',
    source_project_dataset_table=target_table,
    destination_cloud_storage_uris=[output_file],
    compression='GZIP',
    export_format='CSV')

delete_tmp_table = BigQueryTableDeleteOperator(
	dag=dag,
    task_id="delete_tmp_table", 
    bigquery_conn_id='zwift_ga360_bigquery',
    deletion_dataset_table=target_table)

extract_ga360 >> copy_ga360_to_gcs >> delete_tmp_table