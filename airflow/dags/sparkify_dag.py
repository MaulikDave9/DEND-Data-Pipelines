from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# DAG can be browsed from Airflow UI

# DAG contains default_args dict, with the following key:
# Owner, Depends_on_past, Start_date, Retries, Retry_delay, Catchup
default_args = {
    'owner': 'udacity',
    'depends_on_past': False,            # DAG no dependencies on past run
    'start_date': datetime(2019, 1, 12), 
    'retries':  3,                       # on failure, task retired 3 times
    'retry_delay': timedelta(minutes=5), # retries happen every 5 minutes
    'catchup_by_default': False,         # catchup is turned off
    'email_on_retry': False              # do not email on retry
}

# defaults_args are bind to the DAG which is scheduled to run once an hour
dag = DAG('sparkify_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
          max_active_runs= 1
        )


# The dag follows the data flow per project specifications, the tasks have a dependency and 
# DAG begins with a begin_execution task and ends with a end_execution task

start_operator = DummyOperator(
    task_id='Begin_execution', 
    dag=dag
)

# create tables on redshift using DAG
#Concern: will run every hour? IF NOT EXIST in create_table will prevent creating tables every run?
create_tables_task = PostgresOperator(
    task_id='create_tables',
    dag=dag,
    sql='create_tables.sql',
    postgres_conn_id="redshift"
)

# Four different operator will stage the data, tranform the data and run check on data quality

# References: https://knowledge.udacity.com/questions/215210
#             https://knowledge.udacity.com/questions/187917

# Task to stage event data from S3 to Redshift
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    provide_context=True,
    aws_conn_id='aws_credentials',
    redshift_conn_id='redshift',
    table='staging_events',
    s3_bucket= 'udacity_dend',
    s3_key='log_data', 
    region='us-west-2',
    file_type='JSON',
    json_paths='log_json_path.json',
    execution_date='{{ ds }}'
)

# Task to stage song data from S3 to Redshift
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    provide_context=True,
    aws_conn_id='aws_credentials',
    redshift_conn_id='redshift',
    table='staging_songs',
    s3_bucket='udacity-dend',
    s3_key='song_data',
    region='us-west-2',
    file_type='JSON',
    json_paths='auto',
    execution_date='{{ ds }}'

)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag
)

end_operator = DummyOperator(
    task_id='Stop_execution',  
    dag=dag
)

# tasks dependency

start_operator >> create_tables_task >> \
[ stage_events_to_redshift, stage_songs_to_redshift] >> \
load_songplays_table >> [load_song_dimension_table, 
                         load_user_dimension_table,
                         load_artist_dimension_table,
                         load_time_dimension_table] >> \
run_quality_checks >> end_operator