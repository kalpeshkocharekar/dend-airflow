import logging
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
import ast

def to_bool(s):
    return ast.literal_eval(s.capitalize())

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')
bucket = 'dend'
song_key = 'song_data/'
event_key = 'log_data/'

default_args = {
    'owner': 'jay',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
    'catchup': False # Run only the last DAG
}

dag = DAG('s3_to_redshift',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          # schedule_interval='@hourly',
          schedule_interval='0 * * * *'
        )

def begin_execution():
    """ Print out all data from the buckets
    """
    logging.info("Begin Execution")

    # The following code lines were used to debug the S3 connection, once working,
    # they may be commented out.

    hook = S3Hook(aws_conn_id='aws_credentials')
    logging.info(f"Listing Keys from {bucket}/{song_key}")
    keys = hook.list_keys(bucket, prefix=song_key)
    read = False
    for key in keys:
        logging.info(f"- s3://{bucket}/{key}")
        if read == False and key[-4:].upper() == 'JSON':
            content = hook.read_key(key, bucket)
            read = True
            logging.info(content[0:500])

    logging.info(f"Listing Keys from {bucket}/{event_key}")
    keys = hook.list_keys(bucket, prefix=event_key)
    read = False
    for key in keys:
        logging.info(f"- s3://{bucket}/{key}")
        logging.info(f"reading? {read} and {key[-3:].upper()}")
        if read == False and key[-3:].upper() == 'CSV':
            content = hook.read_key(key, bucket)
            read = True
            logging.info(content[0:500])

start_operator = PythonOperator(
    task_id='Begin_execution',
    dag=dag,
    python_callable=begin_execution)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    s3_bucket=bucket,
    s3_key_path=event_key,
    s3_file_type='csv',
    aws_conn_id='aws_credentials',
    redshift_conn_id='redshift',
    redshift_iam_role='redshift_iam_role',
    table='staging_events',
    create_query=SqlQueries.staging_events_table_create,
    copy_query=SqlQueries.staging_events_copy,
    active=to_bool(Variable.get('run_staging'))
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    s3_bucket=bucket,
    s3_key_path=song_key,
    s3_file_type='json',
    aws_conn_id='aws_credentials',
    redshift_conn_id='redshift',
    redshift_iam_role='redshift_iam_role',
    table='staging_songs',
    create_query=SqlQueries.staging_songs_table_create,
    copy_query=SqlQueries.staging_songs_copy,
    active=to_bool(Variable.get('run_staging'))
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift',
    redshift_iam_role='redshift_iam_role',
    table='songplays',
    create_query=SqlQueries.songplay_table_create,
    insert_query=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    redshift_iam_role='redshift_iam_role',
    table='users',
    create_query=SqlQueries.user_table_create,
    insert_query=SqlQueries.user_table_insert,
    append_only=to_bool(Variable.get('append_only'))
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    redshift_iam_role='redshift_iam_role',
    table='songs',
    create_query=SqlQueries.song_table_create,
    insert_query=SqlQueries.song_table_insert,
    append_only=to_bool(Variable.get('append_only'))
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    redshift_iam_role='redshift_iam_role',
    table='artists',
    create_query=SqlQueries.artist_table_create,
    insert_query=SqlQueries.artist_table_insert,
    append_only=to_bool(Variable.get('append_only'))
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    redshift_iam_role='redshift_iam_role',
    table='time',
    create_query=SqlQueries.time_table_create,
    insert_query=SqlQueries.time_table_insert,
    append_only=to_bool(Variable.get('append_only'))
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator