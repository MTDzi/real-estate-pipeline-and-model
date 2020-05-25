from datetime import datetime

from airflow.models import DAG, Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

from args_utils import get_default_args
from quality_checks import check_nullability, check_if_file_exists

import pandas as pd
from pathlib import Path

import logging


default_args = get_default_args()

# The DAG will need the following file to exist at later stages, it doesn't
#  make sense to run the scraping if the file doesn't exist
required_warsaw_map_parquet_filepath = Variable.get(
    'warsaw_map_scraping',
    deserialize_json=True,
)['parquet_dump_filepath']

dag_specific_vars = Variable.get('otodom_scraping', deserialize_json=True)

city = dag_specific_vars['city']
csv_path = dag_specific_vars['csv_path']
year_month_day = datetime.now().strftime('%Y%m%d')
csv_filepath = Path(csv_path) / f'{city}_{year_month_day}.csv'

parquet_path = dag_specific_vars['parquet_path']
parquet_filepath = Path(parquet_path) / f'{city}_{year_month_day}.parquet'

critical_null_percentage = dag_specific_vars['critical_null_percentage']
warning_null_percentage = dag_specific_vars['warning_null_percentage']


def csv_dedup_and_to_parquet(
    csv_filepath: Path,
    parquet_filepath: Path,
) -> None:
    """
    For whatever reason the sqlContext.read.csv function reads in a malformed
    DataFrames, and instead of cleaning them up manually, it's much easier
    to:
        1) read it in using pd.read_csv (which seems to handle them without issues)
        2) cast all mixed-type (object) columns to str
        3) de-duplicate rows
        3) save the frame as a .parquet file
    """
    logging.info(f'Reading in the {csv_filepath} frame')
    df_from_csv = pd.read_csv(csv_filepath)
    object_columns = df_from_csv.select_dtypes(include='object').columns
    df_from_csv[object_columns] = df_from_csv[object_columns].applymap(str)
    df_from_csv = df_from_csv.drop_duplicates()

    logging.info(f'Dumping deduplicated frame to: {parquet_filepath}')
    df_from_csv.to_parquet(
        parquet_filepath,
        index=False,
    )


def column_renaming(parquet_filepath: Path) -> None:
    column_name_translation = {
        'balkon': 'balcony',
        'cena': 'price',
        'czynsz': 'rent',
        'data_pobrania': 'scraped_at',
        'drzwi_okna_anty': 'antiburglar_door_windows',
        'dwupoziomowe': 'two_floors',
        'forma_wlasnosci': 'form_of_property',
        'garaz_miejsce_park': 'garage_parking_spot',
        'klimatyzacja': 'air_conditioning',
        'liczba_pieter': 'number_of_floors_in_building',
        'liczba_pokoi': 'number_of_rooms',
        'material_budynku': 'building_material',
        'monitoring_ochrona': 'surveillance_security',
        'oddzielna_kuchnia': 'separate_kitchen',
        'ogrodek': 'garden',
        'ogrzewanie': 'type_of_heating',
        'okna': 'type_of_windows',
        'opis': 'description',
        'pietro': 'floor',
        'piwnica': 'basement',
        'pom_uzytkowe': 'storage_room',
        'powierzchnia': 'area_in_m2',
        'rodzaj_zabudowy': 'type_of_building',
        'rok_budowy': 'construction_year',
        'rynek': 'market',
        'stan_wykonczenia': 'finish',
        'system_alarmowy': 'alarm_system',
        'taras': 'terrace',
        'teren_zamkniety': 'closed_area',
        'tytul': 'title',
        'winda': 'elevator',
    }
    df = pd.read_parquet(parquet_filepath)
    df.rename(column_name_translation).to_parquet(parquet_filepath)


dag = DAG(
    'otodom_scraping_dag',
    default_args=default_args,
    description='Scrape real estate data from otodom.pl',
    schedule_interval=dag_specific_vars['schedule_interval'],
)

check_if_file_exists_task = PythonOperator(
    dag=dag,
    task_id='check_if_file_exists_task',
    python_callable=lambda: check_if_file_exists(
        required_warsaw_map_parquet_filepath,
        additional_message='!!! You should first run the "warsaw_map_scraping_dag" !!!\n',
    ),
)

otodom_scraping_task = BashOperator(
    dag=dag,
    task_id='otodom_scraping_task',
    bash_command=(
        f'if test -f {csv_filepath};'
        f'then echo "The file {csv_filepath} exists, not scraping otodom"'
        f'else;'
        f'cd /usr/local/airflow/otodom_scraper/;'
        f'../scraper_venv/bin/scrapy crawl otodomSpider -o {csv_filepath} -a city={city} &> /dev/null;'
        f'fi'
    ),
)

csv_dedup_and_to_parquet_task = PythonOperator(
    dag=dag,
    task_id='csv_dedup_and_to_parquet_task',
    python_callable=lambda: csv_dedup_and_to_parquet(csv_filepath, parquet_filepath),
)

check_nullability_task = PythonOperator(
    dag=dag,
    task_id='check_nullability_task',
    python_callable=lambda: check_nullability(
        parquet_filepath,
        critical_null_percentage,
        warning_null_percentage,
    ),
)

column_renaming_task = PythonOperator(
    dag=dag,
    task_id='column_renaming_task',
    python_callable=lambda: column_renaming(parquet_filepath),
)

check_if_file_exists_task >> otodom_scraping_task >> csv_dedup_and_to_parquet_task >> check_nullability_task >> column_renaming_task

