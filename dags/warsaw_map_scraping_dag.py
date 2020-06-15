from airflow.models import DAG, Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

import pandas as pd

from args_utils import get_default_args


def population_density_extraction(
        csv_filepath: str,
        parquet_filepath: str,
) -> None:
    """
    This function extracts the density from the 'r', 'g', and 'b' columns (as a sum of the three),
    selects the relevant columns ('density', 'lon', and 'lat'), and dumps the frame into a .parquet file.
    """
    popul_dens_df = pd.read_csv(csv_filepath)

    # There are parts of the map for which there's no data, we're throwing those away
    popul_dens_df.dropna(inplace=True)

    popul_dens_df['density'] = popul_dens_df[['r', 'g', 'b']].sum(axis=1)
    popul_dens_df[['density', 'lat', 'lon']].to_parquet(parquet_filepath, index=False)


###############################################################################

default_args = get_default_args()
dag_specific_vars = Variable.get('warsaw_map_scraping', deserialize_json=True)

tile_side = dag_specific_vars['tile_side']
csv_dump_filepath = dag_specific_vars['csv_dump_filepath']

###############################################################################

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
        f'if test -f {csv_dump_filepath};'
        f'then echo "The file {csv_dump_filepath} exists, not scraping http://mapa.um.warszawa.pl/";'
        f'else cd /usr/local/airflow/scripts/;../scraper_venv/bin/python warsaw_map_scraper.py -o {csv_dump_filepath} -t {tile_side};'
        f'fi'
    ),
)

population_density_extraction_task = PythonOperator(
    dag=dag,
    task_id='population_density_extraction_task',
    python_callable=population_density_extraction,
    op_kwargs=dict(
        csv_dump_filepath=csv_dump_filepath,
        parquet_dump_filepath=dag_specific_vars['parquet_dump_filepath'],
    ),
)

warsaw_map_scraping_task >> population_density_extraction_task
