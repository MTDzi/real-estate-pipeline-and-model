from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator,
)


default_args = {
    'owner': 'MTDzi',
    'start_date': datetime(2020, 4, 15),
    'email': ['mtdziubinski@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

dag = DAG(
    'airflow_project',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval=None,
)

start_operator = DummyOperator(
    task_id='Begin_execution',
    dag=dag,
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table_name='events_staging',
    s3_url='s3://udacity-dend/log_data',
    redshift_conn_id='redshift',
    aws_conn_id='aws_default',
    json_paths='s3://udacity-dend/log_json_path.json',
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table_name='songs_staging',
    s3_url='s3://udacity-dend/song_data',
    redshift_conn_id='redshift',
    aws_conn_id='aws_default',
    json_paths='auto',
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table_name='songplays',
    redshift_conn_id='redshift',
    aws_conn_id='aws_default',
)

load_users_dimension_table = LoadDimensionOperator(
    task_id='Load_users_dim_table',
    dag=dag,
    table_name='users',
    redshift_conn_id='redshift',
    aws_conn_id='aws_default',
)

load_songs_dimension_table = LoadDimensionOperator(
    task_id='Load_songs_dim_table',
    dag=dag,
    table_name='songs',
    redshift_conn_id='redshift',
    aws_conn_id='aws_default',
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artists_dim_table',
    dag=dag,
    table_name='artists',
    redshift_conn_id='redshift',
    aws_conn_id='aws_default',
    autocommit=True,
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table_name='time',
    redshift_conn_id='redshift',
    aws_conn_id='aws_default',
)

tables_to_check = [
    'artists',
    'time',
    'songplays',
    'songs',
    'users',
]
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    table_names=tables_to_check,
    redshift_conn_id='redshift',
)

end_operator = DummyOperator(
    task_id='End_execution',
    dag=dag,
)

start_operator \
    >> [stage_songs_to_redshift, stage_events_to_redshift] \
    >> load_songplays_table \
    >> [
        load_songs_dimension_table,
        load_artist_dimension_table,
        load_time_dimension_table,
        load_users_dimension_table,
    ] \
    >> run_quality_checks \
    >> end_operator
