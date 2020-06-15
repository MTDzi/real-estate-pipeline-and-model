from datetime import datetime

from airflow.models import DAG, Variable
from airflow.operators.python_operator import PythonOperator

from args_utils import get_default_args
from spark_dedup import add_increment

import pandas as pd
from pathlib import Path

import pyspark
from pyspark.sql import SQLContext

import logging


default_args = get_default_args()

dag_specific_vars = Variable.get('spark_dedup', deserialize_json=True)

city = dag_specific_vars['city']
parquet_input_path = dag_specific_vars['parquet_input_path']
parquet_output_path = dag_specific_vars['parquet_output_path']

dag_specific_vars['schedule_interval'] = '@weekly'
default_args['start_date'] = datetime(2019, 12, 16)
# default_args['end_date'] = datetime(2019, 12, 31)
default_args['retries'] = 0
default_args['max_active_runs'] = 1
dag_specific_vars['catchup'] = True



def rm_tree(pth: Path) -> None:
    for child in pth.iterdir():
        if child.is_file():
            child.unlink()
        else:
            rm_tree(child)
    pth.rmdir()


def spark_merge_deduplicated_rows(
        parquet_input_path: str,
        parquet_output_path: str,
        city: str,
        ds: str,
        **kwargs,
) -> None:
    logging.info('Starting the spark deduplication pipeline')

    spark = (
        pyspark
        .sql
        .SparkSession
        .builder
        .appName('dedup-pipeline')
        .master('local[6]')
        .getOrCreate()
    )

    sc = spark.sparkContext
    sqlContext = SQLContext(sc)

    parquet_input_path = Path(parquet_input_path)
    parquet_output_path = Path(parquet_output_path)

    input_parquets_glob = sorted(parquet_input_path.glob(f'{city}_*.parquet'))
    current_dedup_glob = list(parquet_output_path.glob(f'{city}_*parquet'))

    # Read in the starting dedup file
    assert len(current_dedup_glob) in (0, 1), (
        'There should be either none or one .parquet file'
        ' containing all deduplicated rows'
    )
    if len(current_dedup_glob) == 0:
        origin = input_parquets_glob[0]
        origin_year_month_day = origin.stem.split('_')[1].split('.')[0]
        starting_dedup_filepath = (
                parquet_output_path / f'{city}_{origin_year_month_day}-{origin_year_month_day}.parquet'
        )
        # We open and write the file instead of just copying it, because spark
        #  writes parquet files as directories, not files (as pandas does)
        sqlContext.read.parquet(origin.as_posix()).write.parquet(starting_dedup_filepath.as_posix())
        current_dedup_glob.append(starting_dedup_filepath)

    current_parquet_filepath = current_dedup_glob[0]
    current_df = sqlContext.read.parquet(current_parquet_filepath.as_posix())

    # Extract the start/end date
    start_date, end_date = current_parquet_filepath.stem.split('_')[1].split('-')

    # Go in order through the scraped parquet files and upsert them into
    #  the current deduped frame
    run_year_month_day = ds.replace('-', '')
    new_end_date = None
    for filepath in input_parquets_glob:
        local_end_date = filepath.stem.split('_')[1]
        if local_end_date > run_year_month_day or local_end_date <= end_date:
            logging.info(f'Skipping: {filepath}')
            logging.info(f'run_year_month_day = {run_year_month_day}')
            continue

        new_end_date = local_end_date
        increment_df = sqlContext.read.parquet(filepath.as_posix())
        if set(increment_df.columns) != set(current_df.columns):
            print(f'Column discrepancy: {set(increment_df.columns) - set(current_df.columns)}')
            print(f'Column discrepancy: {set(current_df.columns) - set(increment_df.columns)}')
        current_df = add_increment(current_df, increment_df)

    if new_end_date is None:
        sc.stop()
        return

    # Save the deduplicated frame as a parquet file
    filename = f'{city}_{start_date}-{new_end_date}.parquet'
    current_df.write.parquet((parquet_output_path / filename).as_posix())

    # Delete the previous parquet file
    rm_tree(current_parquet_filepath)

    # Stop the spark context
    sc.stop()


dag = DAG(
    'spark_dedup_dag',
    default_args=default_args,
    description='Combine scraped data into a deduplicated .parquet file',
    schedule_interval=dag_specific_vars['schedule_interval'],
    max_active_runs=1,
)

spark_merge_deduplicated_rows_task = PythonOperator(
    dag=dag,
    task_id='spark_merge_deduplicated_rows_task',
    provide_context=True,
    python_callable=spark_merge_deduplicated_rows,
    op_kwargs=dict(
        parquet_input_path=parquet_input_path,
        parquet_output_path=parquet_output_path,
        city=city,
    ),
)
