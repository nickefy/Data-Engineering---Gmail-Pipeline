from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from os import environ
from google.cloud import bigquery
from datetime import datetime, timedelta
import logging
import time


class CheckBQDuplication(BaseOperator):
    """
    Check if a specific table in BigQuery contains duplicated data after the load
    """

    @apply_defaults
    def __init__(
        self,
        dataset_name,
        bigquery_table_name,
        bigquery_table_key,
        date_column,
        *args, **kwargs):

        super(CheckBQDuplication, self).__init__(*args, **kwargs)
        self.dataset_name = dataset_name
        self.bigquery_table_name = bigquery_table_name
        self.bigquery_table_key = bigquery_table_key
        self.local_path = 'File Path of CSV'
        self.date_column = date_column


    def __check_BQ_duplication(self,execution_date):
        # Check if the table contains duplicated data

        check_query = """
            SELECT MAX(count) FROM (
            SELECT $ID_COLUMN, COUNT(*) as count
            FROM `$DATASET_NAME.$BIGQUERY_TABLE_NAME`
            WHERE CAST($DATE_COLUMN AS DATE) = $EXECUTION_DATE
            GROUP BY $ID_COLUMN)
            """
        check_query = check_query \
                    .replace("$ID_COLUMN", self.bigquery_table_key) \
                    .replace("$DATASET_NAME", self.dataset_name) \
                    .replace("$BIGQUERY_TABLE_NAME", self.bigquery_table_name) \
                    .replace("$EXECUTION_DATE", "'" + execution_date + "'") \
                    .replace("$DATE_COLUMN", self.date_column)
        logging.info("CHECK QUERY : %s" % check_query)


        check_query_job = bigquery.Client().query(query = check_query)
        logging.info("job state : %s at %s" % (check_query_job.state, check_query_job.ended))

        check_query_result = 0
        for row in check_query_job:
            check_query_result = int(row[0])
            break
    
        if check_query_result > 1:
            logging.error('(ERROR): DUPLICATION EXISTS IN TABLE ' + self.bigquery_table_name)
 
            # Duplication exists, proceed to delete the data in Big Query
            delete_query = """
                DELETE FROM `$DATASET_NAME.$BIGQUERY_TABLE_NAME`
                WHERE CAST($DATE_COLUMN AS DATE) = $EXECUTION_DATE
                """
            delete_query = delete_query \
                            .replace("$DATASET_NAME", self.dataset_name) \
                            .replace("$BIGQUERY_TABLE_NAME", self.bigquery_table_name) \
                            .replace("$EXECUTION_DATE", "'" + execution_date + "'") \
                .replace("$DATE_COLUMN", self.date_column)
            logging.info("DELETE QUERY : %s" % delete_query)

            delete_query_job = bigquery.Client().query(query=delete_query)
            logging.info("job state : %s at %s" % (delete_query_job.state, delete_query_job.ended))

            # Reload the file from Cloud Storage
            print('going through the folder')
            for file in os.listdir(self.local_path):
                file_path_to_load = self.local_path + 'cleaned_' + file    

                logging.info("FULL FILE PATH TO LOAD : %s" % file_path_to_load)

                client = bigquery.Client()
                dataset_ref = client.dataset(self.dataset_name)
                job_config = bigquery.LoadJobConfig()
                job_config.autodetect = False
                job_config.write_disposition = 'WRITE_APPEND'
                job_config.skip_leading_rows = 1
                job_config.field_delimiter = ','
                job_config.quote = ''
                job_config.allow_quoted_newlines = True
                with open( file_path_to_load, 'rb' ) as source_file:
                    load_job = client.load_table_from_file(
                            source_file,
                            dataset_ref.table(self.bigquery_table_name),
                            job_config=job_config)
                assert load_job.job_type == 'load'
                load_job.result()
                assert load_job.state == 'DONE'
            
    def execute(self, context):
        execution_date = (context.get('execution_date') 
        self.__check_BQ_duplication(execution_date)
