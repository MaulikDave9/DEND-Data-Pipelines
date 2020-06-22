from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'depends_on_past': False,            # DAG no dependencies on past run
    'start_date': datetime(2019, 1, 12), 
    'retries':  3,                       # on failure, task retired 3 times
    'retry_delay': timedelta(minutes=5), # Retries happen every 5 minutes
    'catchup_by_default': False,         # Catchup is turned off
    'email_on_retry': False              # Do not email on retry
}

dag = DAG('sparkify_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
          max_active_runs= 1
        )

# Four different operator will stage the data, tranform the data and run check on data quality
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# Stage operator will load any JSON formatted files from S3 to Amazon Redshift
# it will create and run SQL COPY statement based on parameters. 

# References: https://knowledge.udacity.com/questions/215210

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    provide_context=True,
    aws_conn_id='aws_credentials',
    db_conn_id='redshift',
    table='staging_events',
    data_path='s3://udacity-dend/log_data',
    jsonpaths_path= 's3://udacity-dend/log_json_path.json'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag
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

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)
