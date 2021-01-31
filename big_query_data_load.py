from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.file_to_gcs import FileToGoogleCloudStorageOperator
from airflow.utils.dates import days_ago
from airflow.operators.email_operator import EmailOperator
from airflow.utils.email import send_email
import logging as log
import os
import pandas as pd
from google.cloud import bigquery
import google.cloud.bigquery as bq
import regex as re

client = bq.Client.from_service_account_json("../Apps/airflow/secrets/key.json")
sql = """
SELECT

MAX(Date.Date)

FROM

company_data_mart_dev.Date

"""

df = client.query(sql).to_dataframe()
date_str = df.values[0][0].strftime('%d %m %Y')
date_str = re.sub('[^A-Za-z0-9]+', '_', date_str)

PROJECT_ID = Variable.get('project')
LANDING_BUCKET = Variable.get('landing_bucket')
OWNER = Variable.get('owner')
ALERT_EMAIL = Variable.get('alert_email')
BQ_ENV = Variable.get('big_query_environment')
STAR_SCHEMA_DIR = Variable.get('star_schema_location')
SCRIPTS_LOCATION = Variable.get('scripts_location')

default_arguments = {'owner': OWNER, 'start_date': days_ago(1)}



email_body = """

Hello,
<br>
<br>
Please find your latest intelligence reports attached to this email.
<br>
<br>
If you have any questions please reply back to this email address.
<br>
<br>
Kind regards,
<br>

<br>

"""



def notify_email_failure(contextDict, **kwargs):
    """Send custom email alerts."""

    # email title.
    title = "Airflow alert: {task_instance.task_id} Failed".format(
        **contextDict)

    # email contents
    body = """
    Hi Everyone, <br>
    <br>
    There's been an error in refreshing your data.

    Technical Details:
    DAG_ID = {task_instance.dag_id}
    TASK_ID = {task_instance.task_id}.
    <br>
    <br>
    Thanks,<br>
    Airflow Management <br>
    """.format(**contextDict)

    send_email(ALERT_EMAIL, title, body)


def upload_objects(destination_bucket=None, prefix=None, **kwargs):
    directory = STAR_SCHEMA_DIR
    files = files = [x for x in os.listdir(directory)]

    hook = GoogleCloudStorageHook()

    for file in files:
        hook.upload(bucket=LANDING_BUCKET, object=file,
                    filename=directory+file)


with DAG(
    'big_query_data_load',
    schedule_interval='@daily',
    catchup=False,
    default_args=default_arguments,
    max_active_runs=1,
    user_defined_macros={'project': PROJECT_ID}
) as dag:

    bash_extract_data = BashOperator(
        task_id='bash_extract_data',
        bash_command='python3 ' + SCRIPTS_LOCATION+'1.ExtractData.py',
        on_failure_callback=notify_email_failure
    )

    bash_cleanse_data = BashOperator(
        task_id='bash_cleanse_data',
        bash_command='python3 ' + SCRIPTS_LOCATION+'2.CleanseData.py',
        on_failure_callback=notify_email_failure
    )

    bash_transform_data = BashOperator(
        task_id='bash_transform_data',
        bash_command='python3 ' + SCRIPTS_LOCATION+'3.TransformData.py',
        on_failure_callback=notify_email_failure
    )

    gcs_load_data = PythonOperator(
        task_id='gcs_load_data',
        python_callable=upload_objects,
        on_failure_callback=notify_email_failure
    )

    load_sales_data = GoogleCloudStorageToBigQueryOperator(
        task_id='load_sales_data',
        bucket=LANDING_BUCKET,
        source_objects=['Sales.csv'],
        skip_leading_rows=1,
        field_delimiter=',',
        destination_project_dataset_table=PROJECT_ID+'.'+BQ_ENV+'.Sales',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        bigquery_conn_id='google_cloud_default',
        google_cloud_storage_conn_id='google_cloud_default',
        on_failure_callback=notify_email_failure
    )

    load_dim_date = GoogleCloudStorageToBigQueryOperator(
        task_id='load_dim_date',
        bucket=LANDING_BUCKET,
        source_objects=['Date.csv'],
        skip_leading_rows=1,
        field_delimiter=',',
        destination_project_dataset_table=PROJECT_ID+'.'+BQ_ENV+'.Date',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        bigquery_conn_id='google_cloud_default',
        google_cloud_storage_conn_id='google_cloud_default',
        on_failure_callback=notify_email_failure
    )

    load_dim_item = GoogleCloudStorageToBigQueryOperator(
        task_id='load_dim_item',
        bucket=LANDING_BUCKET,
        source_objects=['Item.csv'],
        skip_leading_rows=1,
        field_delimiter=',',
        destination_project_dataset_table=PROJECT_ID+'.'+BQ_ENV+'.Item',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        bigquery_conn_id='google_cloud_default',
        google_cloud_storage_conn_id='google_cloud_default',
        on_failure_callback=notify_email_failure
    )

    load_dim_status = GoogleCloudStorageToBigQueryOperator(
        task_id='load_dim_status',
        bucket=LANDING_BUCKET,
        source_objects=['Status.csv'],
        skip_leading_rows=1,
        field_delimiter=',',
        destination_project_dataset_table=PROJECT_ID+'.'+BQ_ENV+'.Status',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        bigquery_conn_id='google_cloud_default',
        google_cloud_storage_conn_id='google_cloud_default',
        on_failure_callback=notify_email_failure
    )

    bash_generate_assets = BashOperator(
        task_id='bash_generate_assets',
        bash_command='python3 ' + SCRIPTS_LOCATION+'report_assets.py',
        on_failure_callback=notify_email_failure
    )

    bash_generate_reports = BashOperator(
        task_id='bash_generate_reports',
        bash_command='python3 ' + SCRIPTS_LOCATION+'driver.py',
        on_failure_callback=notify_email_failure
    )

    email_reports = EmailOperator(
        task_id='email_reports',
        to=[''],
        subject='Sales Reports',
        html_content=email_body,
        files=['.../airflow/reporting_env/output/'+date_str+'_Top_5_by_least_cover.pdf',
        '/home/alexander/Documents/Apps/airflow/reporting_env/output/'+date_str+'_Top_5_Last_Week_Sales.pdf']
        )




bash_extract_data >> bash_cleanse_data >> bash_transform_data >> gcs_load_data >> load_sales_data >> load_dim_date >> load_dim_item >> load_dim_status >> bash_generate_assets >> bash_generate_reports >> email_reports
