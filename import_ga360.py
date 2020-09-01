from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.operators.bigquery_table_delete_operator import BigQueryTableDeleteOperator
from airflow.contrib.operators.gcs_to_s3 import GoogleCloudStorageToS3Operator
from airflow.operators.s3_to_redshift_operator import S3ToRedshiftTransfer
from airflow.utils.dates import days_ago
from datetime import timedelta

output_file = '{{ ds_nodash }}.csv.gz'
s3_bucket = 'airflow'
s3_output_file = 's3://' + s3_bucket + '/' + output_file
gcs_bucket = 'bq-ga360-dumps'
gcs_output_file = 'gs://' + gcs_bucket + '/' + output_file
bigquery_project_id = '104737153'
source_table = bigquery_project_id + '.ga_sessions_{{ ds_nodash }}'
target_table = bigquery_project_id + '.ga_tmp_{{ ds_nodash }}'

# [START composer_notify_failure]
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('ga360_etl',
          schedule_interval=timedelta(days=1),
          default_args=default_args)

prepare_ga360 = BigQueryOperator(
    dag=dag,
    task_id='bq_unnest_table',
    bigquery_conn_id='zwift_ga360_bigquery',
    sql="""SELECT visitId, clientId FROM `{source_table}`""".format(source_table=source_table),
    use_legacy_sql=False,
    destination_dataset_table=target_table
)

extract_ga360_to_gcs = BigQueryToCloudStorageOperator(
    dag=dag,
    task_id='export_recent_yesterday_to_gcs',
    bigquery_conn_id='zwift_ga360_bigquery',
    source_project_dataset_table=target_table,
    destination_cloud_storage_uris=[gcs_output_file],
    compression='GZIP',
    export_format='CSV'
)

export_gcs_to_s3 = GoogleCloudStorageToS3Operator(
    dag=dag,
    task_id="cp_gcs_to_s3",
    dest_verify=False,
    google_cloud_storage_conn_id='zwift_ga360_bigquery',
    bucket=gcs_bucket,
    dest_aws_conn_id='local_s3',
    dest_s3_key='s3://' + s3_bucket
)

load_redshift = S3ToRedshiftTransfer(
    dag=dag,
    task_id="redshift_load",
    s3_bucket=s3_bucket,
    s3_key=output_file,
)

delete_tmp_table = BigQueryTableDeleteOperator(
    dag=dag,
    task_id="delete_tmp_table",
    bigquery_conn_id='zwift_ga360_bigquery',
    deletion_dataset_table=target_table
)

prepare_ga360 >> extract_ga360_to_gcs >> export_gcs_to_s3
