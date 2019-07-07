from airflow.exceptions import AirflowException
from airflow.operators import ShortCircuitOperator
from airflow import models
from airflow import DAG
from operators import GmailToGCS
from operators import StorageToBQ
from operators import CheckDupBQ
from operators import WriteLogs
from operators import SendEmail
from airflow.utils.email import send_email
import os
from datetime import datetime, timedelta

yesterday = datetime.combine(
    datetime.today() - timedelta(1),
    datetime.min.time())

#please fill in the values required
#interval of the dag run
# datetime.timedelta(days=1)
# '0 2 1 * *' ' min hour dayofmonth month dayofweek'
schedule_interval_dag = timedelta(weeks=1)

#name of the inbox folder to be extracted
inbox_name_dag = 'inbox_label_name'

#name of table in BQ
bigquery_table_name_dag = 'bigquery_table_name'

#dataset name in BQ
dataset_name_dag = 'bigquery_dataset_name'

#write mode : append,overwrite
write_mode_dag = 'append'

# table key to check for duplicates
bigquery_table_key_dag = 'bigquery_table_key'

# date column to reupload if there is duplicates
date_column_dag = 'date_column'

# file path to be loaded
file_path_dag = 'file path to be loaded'


def notify_email(contextDict, **kwargs):
    """Send custom email alerts."""

    # email title.
    title = "Airflow ERROR : {dag} Failed".format(**contextDict)

    # email contents
    body = """
    IMPORTANT, <br>
    <br>
    There's been an error in the {dag} job.<br>
    <br>
    Airflow bot <br>
    """.format(**contextDict)

    send_email('youremail@gmail.com', title, body)

def checkforfile():
    if os.listdir(file_path_dag):
        return True
    else:    
        return False

default_dag_args = {
    # Setting start date as yesterday starts the DAG immediately when it is
    # detected in the Cloud Storage bucket.
    # set your start_date : airflow will run previous dags if dags since startdate has not run
    'start_date': datetime(2019, 7, 1, 10),
    'email_on_failure': True,
    'email_on_retry': True,
    'project_id' : 'your project id',
    'on_failure_callback': notify_email,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with models.DAG(
    dag_id='your dag name',
    # Continue to run DAG once per day
    schedule_interval = schedule_interval_dag,
    catchup = True,
    default_args=default_dag_args) as dag:

    email_to_gcs = GmailToGCS.ExtractAttachment(
        task_id='email_to_gcs',
        inbox_name= inbox_name_dag)

    checkforfile = ShortCircuitOperator(
        task_id='checkforfile',
        provide_context=False,
        python_callable=checkforfile)

    gcs_to_bq = StorageToBQ.StorageToBigQuery(
        task_id='gcs_to_bq',
        dataset_name = dataset_name_dag,
        bigquery_table_name = bigquery_table_name_dag,
        write_mode = write_mode_dag)

    check_dups = CheckDupBQ.CheckBQDuplication(
      task_id= 'check_dups',
      dataset_name = dataset_name_dag,
      bigquery_table_name = bigquery_table_name_dag,
      bigquery_table_key = bigquery_table_key_dag,
      date_column = date_column_dag)

    write_logs = WriteLogs.WriteLogToBigQuery(
      task_id = 'write_logs',
      bigquery_table_name = bigquery_table_name_dag)

    SendEmail = SendEmail.SendNotificationEmail(
        task_id='send_email',
        dataset_name = dataset_name_dag,
        bigquery_table_name = bigquery_table_name_dag,
        date_column = date_column_dag)

    email_to_gcs 
    email_to_gcs.set_downstream([checkforfile])
    checkforfile.set_downstream([gcs_to_bq])
    gcs_to_bq >> check_dups >> write_logs >> SendEmail











