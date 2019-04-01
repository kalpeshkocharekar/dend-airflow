from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.models import Variable
import logging

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    @apply_defaults
    def __init__(self,
                 s3_bucket='',
                 s3_key_path='',
                 s3_file_type='csv', # csv or json
                 aws_conn_id='',
                 redshift_conn_id='',
                 redshift_iam_role='',
                 table='', # Staging table to be saved in database.
                 create_query='', # Query to run to create the table.
                 copy_query='',
                 active=True,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.s3_bucket = s3_bucket
        self.s3_key_path = s3_key_path
        self.s3_file_type = s3_file_type
        self.aws_conn_id = aws_conn_id
        self.redshift_conn_id = redshift_conn_id
        self.redshift_iam_role = redshift_iam_role
        self.table = table
        self.create_query = create_query
        self.copy_query = copy_query
        self.active = active

    def execute(self, context):
        if self.active:
            # Hook to access S3 to grab the raw datafiles.
            s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)

            # Hook to access Amazon RedShift for the copy operation.
            redshift_hook = PostgresHook(self.redshift_conn_id)

            # Recreate table.
            redshift_hook.run(self.create_query)

            # Use AWS hook to access S3 path that contains CSV or JSON files.
            # We will then copy these files onto our tables in RedShift.
            keys = s3_hook.list_keys(self.s3_bucket, prefix=self.s3_key_path)
            for key in keys:
                s3_path = f"s3://{self.s3_bucket}/{key}"
                logging.info(f"Found {s3_path}")
                logging.info(f"Comparing {s3_path[-len(self.s3_file_type):].upper()} and {self.s3_file_type.upper()}")
                # Check if extension is the same with specified file type.
                if s3_path[-len(self.s3_file_type):].upper() == self.s3_file_type.upper():
                    logging.info("copying...")
                    # Copy to RedShift
                    redshift_hook.run(self.copy_query.format(
                        s3_path,
                        Variable.get(self.redshift_iam_role)))
