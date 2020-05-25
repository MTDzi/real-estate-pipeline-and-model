from airflow.models import DAG, Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

import pandas as pd

from args_utils import get_default_args
from quality_checks import check_nullability


default_args = get_default_args()
dag_specific_vars = Variable.get('warsaw_map_scraping', deserialize_json=True)

tile_side = dag_specific_vars['tile_side']
csv_dump_filepath = dag_specific_vars['csv_dump_filepath']
parquet_dump_filepath = dag_specific_vars['parquet_dump_filepath']

critical_null_percentage = dag_specific_vars['critical_null_percentage']
warning_null_percentage = dag_specific_vars['warning_null_percentage']


def cleanup_and_to_parquet(
        csv_filepath: str,
        parquet_filepath: str,
) -> None:
    """
    This function extracts the density from the 'r', 'g', and 'b' columns (as a sum of the three),
    selects the relevant columns ('density', 'lon', and 'lat'), and dumps the frame into a .parquet file.
    """
    popul_dens_df = pd.read_csv(csv_filepath)
    popul_dens_df['density'] = popul_dens_df[['r', 'g', 'b']].sum(axis=1)
    popul_dens_df[['density', 'lat', 'lon']].to_parquet(parquet_filepath, index=False)


dag = DAG(
    dag_id='warsaw_map_scraping',
    default_args=default_args,
    catchup=False,
    schedule_interval=dag_specific_vars['schedule_interval'],
)

warsaw_map_scraping_task = BashOperator(
    dag=dag,
    task_id='warsaw_map_scraping_task',
    bash_command=(
        'cd /usr/local/airflow/scripts/;'
        f' ../scraper_venv/bin/python warsaw_map_scraper.py -o {csv_dump_filepath} -t {tile_side}'
    ),
)

cleanup_and_to_parquet_task = PythonOperator(
    dag=dag,
    task_id='cleanup_and_to_parquet_task',
    python_callable=lambda: cleanup_and_to_parquet(
        csv_dump_filepath,
        parquet_dump_filepath,
    ),
)

check_nullability_task = PythonOperator(
    dag=dag,
    task_id='check_nullability_task',
    python_callable=lambda: check_nullability(
        parquet_dump_filepath,
        critical_null_percentage,
        warning_null_percentage,
    )
)

warsaw_map_scraping_task >> cleanup_and_to_parquet_task >> check_nullability_task