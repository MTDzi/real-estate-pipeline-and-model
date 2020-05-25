from airflow.models import DAG, Variable
from airflow.operators.bash_operator import BashOperator

from args_utils import get_default_args


default_args = get_default_args()
dag_specific_vars = Variable.get('warsaw_map_scraping', deserialize_json=True)

dag = DAG(
    dag_id='warsaw_map_scraping',
    default_args=default_args,
    catchup=False,
    schedule_interval=dag_specific_vars['schedule_interval'],
)

tile_side = dag_specific_vars['tile_side']
csv_dump_filename = dag_specific_vars['csv_dump_filename']
warsaw_map_scraping_task = BashOperator(
    dag=dag,
    task_id='run_warsaw_map_scraping_script',
    bash_command=(
        'cd /usr/local/airflow/scripts/;'
        f' ../scraper_venv/bin/python warsaw_map_scraper.py -o {csv_dump_filename} -t {tile_side}'
    ),
)
