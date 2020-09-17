from airflow import DAG
from airflow.models import Variable
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.operators.bigquery_table_delete_operator import BigQueryTableDeleteOperator
from airflow.contrib.operators.gcs_to_s3 import GoogleCloudStorageToS3Operator
from airflow.utils.dates import days_ago
from zwift.s3_to_redshift_transfer import S3ToRedshiftTransfer
from datetime import timedelta

google_conn_id = Variable.get("ga360_conn_id")
bigquery_project_id = Variable.get("ga360_bigquery_project_id")
gcs_bucket = Variable.get("ga360_bucket")
redshift_s3_bucket = 's3://' + Variable.get("redshift_s3_bucket") + '/'
redshift_conn_id = Variable.get("redshift_conn_id")
redshift_iam_role = Variable.get("redshift_iam_role")
aws_s3_conn_id = Variable.get("s3_conn_id")

output_file = 'ga360-sessions-{{ ds_nodash }}.csv.gz'
s3_output_file = redshift_s3_bucket + output_file
gcs_output_file = 'gs://' + gcs_bucket + '/' + output_file

source_table = bigquery_project_id + '.ga_sessions_{{ ds_nodash }}'
target_table = bigquery_project_id + '.ga_tmp_{{ ds_nodash }}'

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

bq_extract_query = """with sessions as (select * from `{source_table}`), 
                           userId as (select visitId, dimension.value as userId, clientId
                                      from sessions cross join unnest(customDimensions) as dimension
                                      where dimension.index = 10), 
                           data as (select userId.userId, sessions.fullVisitorId, sessions.clientId, sessions.visitId, sessions.hits, sessions.visitStartTime, sessions.date 
                                    from sessions left join userId 
                                        on sessions.visitId = userId.visitId
                                        and sessions.clientId = userId.clientId)
                      select userId as distinct_id, clientId, visitId, visitStartTime, date, fullVisitorId, hit.hitNumber, hit.page.pagePath, hit.appInfo.screenName , hit.page.hostname, hit.page.pageTitle, hit.type as hit_type
                      from data cross join unnest(hits) as hit"""

prepare_ga360 = BigQueryOperator(
    dag=dag,
    task_id='bq_unnest_table',
    bigquery_conn_id=google_conn_id,
    use_legacy_sql=False,
    sql=bq_extract_query.format(source_table=source_table),
    destination_dataset_table=target_table
)

extract_ga360_to_gcs = BigQueryToCloudStorageOperator(
    dag=dag,
    task_id='export_recent_yesterday_to_gcs',
    bigquery_conn_id=google_conn_id,
    source_project_dataset_table=target_table,
    destination_cloud_storage_uris=[gcs_output_file],
    compression='GZIP',
    export_format='CSV'
)

export_gcs_to_s3 = GoogleCloudStorageToS3Operator(
    dag=dag,
    task_id="cp_gcs_to_s3",
    dest_verify=True,
    google_cloud_storage_conn_id=google_conn_id,
    bucket=gcs_bucket,
    dest_aws_conn_id='local_s3',
    dest_s3_key=redshift_s3_bucket
)

load_redshift = S3ToRedshiftTransfer(
    dag=dag,
    task_id="redshift_load",
    redshift_conn_id=redshift_conn_id,
    s3_file=s3_output_file,
    schema='public',
    table='ga360_sessions',
    iam_role=redshift_iam_role,
    copy_options=['CSV', 'IGNOREHEADER 1', 'GZIP', """DATEFORMAT AS 'YYYYMMDD'"""]
)

delete_tmp_table = BigQueryTableDeleteOperator(
    dag=dag,
    task_id="delete_tmp_table",
    bigquery_conn_id=google_conn_id,
    deletion_dataset_table=target_table
)

prepare_ga360 >> extract_ga360_to_gcs
extract_ga360_to_gcs >> [export_gcs_to_s3, delete_tmp_table]
export_gcs_to_s3 >> load_redshift
