from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 8, 26),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('minio_test', default_args=default_args, schedule_interval="@once")


def write_text_file(ds, **kwargs):
    with open("/tmp/test.txt", "w") as fp:
        # Add file generation/processing step here, E.g.:
        fp.write(ds)

        # Upload generated file to Minio
        s3 = S3Hook('local_s3')
        s3.load_file("/tmp/test.txt",
                     key=f"my-test-file.txt",
                     bucket_name="airflow-forwards")


# Create a task to call your processing function
t1 = PythonOperator(
    task_id='gen_and_upload_to_s3',
    provide_context=True,
    python_callable=write_text_file,
    dag=dag
)