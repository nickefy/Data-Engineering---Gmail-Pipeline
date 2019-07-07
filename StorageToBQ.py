from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from os import environ
from datetime import timedelta
from google.cloud import bigquery
import pandas as pd
import logging
import os


class StorageToBigQuery(BaseOperator):
    """
    Load file from Google Cloud Storage to Google Big Query
    """

    @apply_defaults
    def __init__(
            self,
            dataset_name,
            bigquery_table_name,
            write_mode,
            local_path = 'File Path of CSV',
            *args, **kwargs):

        super(StorageToBigQuery, self).__init__(*args, **kwargs)
        self.dataset_name =  dataset_name
        self.bigquery_table_name =  bigquery_table_name
        self.write_mode = write_mode
        self.local_path = local_path

    def __StorageToBigQuery(self, execution_date):
        print('going through the folder')
        for file in os.listdir(self.local_path):
            print(file)
            filename = self.local_path + file
            print(filename)
            df=pd.read_csv(filename,error_bad_lines=False)
            # cleaning begins
            # cleaning ends
            df.to_csv(self.local_path + 'cleaned_' + file,index=False)


            file_path_to_load = self.local_path + 'cleaned_' + file
            logging.info("FILE PATH TO LOAD : %s" % file_path_to_load)
            print('loading_file_to_BQ')
            client = bigquery.Client()
            dataset_ref = client.dataset(self.dataset_name)
            job_config = bigquery.LoadJobConfig()
            job_config.autodetect = False
            if(self.write_mode == 'overwrite'):
                job_config.write_disposition = 'WRITE_TRUNCATE'
            elif(self.write_mode == 'empty'):
                job_config.write_disposition = 'WRITE_EMPTY'
            else:
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
        self.__StorageToBigQuery(execution_date)
