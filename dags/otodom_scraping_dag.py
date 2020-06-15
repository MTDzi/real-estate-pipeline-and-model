from datetime import datetime

from airflow.models import DAG, Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

from args_utils import get_default_args
from quality_checks import check_nullability, check_if_file_exists
from rough_join import find_closest_density

import numpy as np
import pandas as pd
from pathlib import Path

import logging


default_args = get_default_args()

# The DAG will need the following file to exist at later tasks, it doesn't
#  make sense to run the scraping if the file doesn't exist
required_warsaw_map_parquet_filepath = Variable.get(
    'warsaw_map_scraping',
    deserialize_json=True,
)['parquet_dump_filepath']

dag_specific_vars = Variable.get('otodom_scraping', deserialize_json=True)

city = dag_specific_vars['city']
csv_path = dag_specific_vars['csv_path']

parquet_path = dag_specific_vars['parquet_path']

critical_null_percentage = dag_specific_vars['critical_null_percentage']
warning_null_percentage = dag_specific_vars['warning_null_percentage']



critical_null_percentage = 101
default_args['start_date'] = datetime(2020, 1, 8)
default_args['end_date'] = datetime(2020, 1, 10)
default_args['retries'] = 0
dag_specific_vars['schedule_interval'] = '@daily'
dag_specific_vars['catchup'] = True





def csv_dedup_and_to_parquet(
    csv_path: str,
    city: str,
    parquet_path: str,
    ds: str,
    **kwargs,
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
    year_month_day = ds.replace('-', '')
    csv_filepath = Path(csv_path) / f'{city}_{year_month_day}.csv'
    logging.info(f'Reading in the {csv_filepath} frame')
    df_from_csv = pd.read_csv(csv_filepath)
    object_columns = df_from_csv.select_dtypes(include='object').columns
    df_from_csv[object_columns] = df_from_csv[object_columns].applymap(str)
    df_from_csv = df_from_csv.drop_duplicates()

    parquet_filepath = Path(parquet_path) / f'{city}_{year_month_day}.parquet'
    logging.info(f'Dumping deduplicated frame to: {parquet_filepath}')
    df_from_csv.to_parquet(
        parquet_filepath,
        index=False,
    )


def lat_lon_cleanup(
    parquet_path: str,
    city: str,
    ds: str,
    **kwargs,
):
    logging.info('Reading in the otodom and the warsaw map parquet files')
    year_month_day = ds.replace('-', '')
    parquet_filepath = Path(parquet_path) / f'{city}_{year_month_day}.parquet'
    df = pd.read_parquet(parquet_filepath)

    def cast_to_float_or_nan(x):
        try:
            return float(x)
        except ValueError:
            return np.nan

    for col in ['lat', 'lon']:
        df[col] = df[col].apply(cast_to_float_or_nan)
    df.to_parquet(parquet_filepath, index=False)


def rough_join_with_map_data(
    parquet_path: str,
    city: str,
    warsaw_map_parquet_filepath: Path,
    ds: str,
    **kwargs,
) -> None:
    logging.info('Reading in the otodom and the warsaw map parquet files')
    year_month_day = ds.replace('-', '')
    parquet_filepath = Path(parquet_path) / f'{city}_{year_month_day}.parquet'
    otodom_df = pd.read_parquet(parquet_filepath)
    popul_dens_df = pd.read_parquet(warsaw_map_parquet_filepath)
    logging.info('Adding the "population_density" column to the otodom frame')
    otodom_df['population_density'] = (
        otodom_df[['lon', 'lat']]
        .apply(lambda row: find_closest_density(row, popul_dens_df), axis=1)
    )
    logging.info(f'Done, now saving the results to {parquet_filepath}')
    otodom_df.to_parquet(parquet_filepath, index=False)


def column_renaming(
        parquet_path: str,
        city: str,
        ds: str,
        **kwargs,
) -> None:
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
    year_month_day = ds.replace('-', '')
    parquet_filepath = Path(parquet_path) / f'{city}_{year_month_day}.parquet'
    df = pd.read_parquet(parquet_filepath)
    df.rename(columns=column_name_translation, inplace=True)
    df.to_parquet(parquet_filepath, index=False)


def column_selection(
        parquet_path: str,
        city: str,
        ds: str,
        **kwargs
) -> None:
    """
    This function drops descriptive columns, meaning: those on which I would need
    to use some more sophisticated NLP methods to extract any useful information.
    """
    columns_to_drop = [
        'description',
        'title',
    ]
    year_month_day = ds.replace('-', '')
    parquet_filepath = Path(parquet_path) / f'{city}_{year_month_day}.parquet'
    df = pd.read_parquet(parquet_filepath)
    df.drop(columns_to_drop, axis=1, inplace=True)
    df.to_parquet(parquet_filepath, index=False)


dag = DAG(
    'otodom_scraping_dag',
    default_args=default_args,
    description='Scrape real estate data from otodom.pl',
    schedule_interval=dag_specific_vars['schedule_interval'],
)

check_if_file_exists_task = PythonOperator(
    dag=dag,
    task_id='check_if_file_exists_task',
    python_callable=check_if_file_exists,
    op_kwargs=dict(
        filepath=required_warsaw_map_parquet_filepath,
        additional_message='!!! You should first run the "warsaw_map_scraping_dag" !!!\n',
    )
)

otodom_scraping_task = BashOperator(
    dag=dag,
    task_id='otodom_scraping_task',
    bash_command=(
        'echo "siema"'
        # f'if test -f {csv_filepath};'
        # f'then echo "The file {csv_filepath} exists, not scraping otodom";'
        # f'else #cd /usr/local/airflow/otodom_scraper/;../scraper_venv/bin/scrapy crawl otodomSpider -o {csv_filepath} -a city={city} &> /dev/null;'
        # f'fi'
    ),
)

csv_dedup_and_to_parquet_task = PythonOperator(
    dag=dag,
    task_id='csv_dedup_and_to_parquet_task',
    provide_context=True,
    python_callable=csv_dedup_and_to_parquet,
    op_kwargs=dict(
        csv_path=csv_path,
        city=city,
        parquet_path=parquet_path,
    ),
)

lat_lon_cleanup_task = PythonOperator(
    dag=dag,
    task_id='lat_lon_cleanup_task',
    provide_context=True,
    python_callable=lat_lon_cleanup,
    op_kwargs=dict(
        csv_path=csv_path,
        city=city,
        parquet_path=parquet_path,
    ),
)

check_nullability_task = PythonOperator(
    dag=dag,
    task_id='check_nullability_task',
    provide_context=True,
    python_callable=check_nullability,
    op_kwargs=dict(
        parquet_path=parquet_path,
        city=city,
        critical_null_percentage=critical_null_percentage,
        warning_null_percentage=warning_null_percentage,
    ),
)

rough_join_with_map_data_task = PythonOperator(
    dag=dag,
    task_id='rough_join_with_map_data_task',
    provide_context=True,
    python_callable=rough_join_with_map_data,
    op_kwargs=dict(
        parquet_path=parquet_path,
        city=city,
        warsaw_map_parquet_filepath=required_warsaw_map_parquet_filepath,
    )
)

column_renaming_task = PythonOperator(
    dag=dag,
    task_id='column_renaming_task',
    provide_context=True,
    python_callable=column_renaming,
    op_kwargs=dict(
        parquet_path=parquet_path,
        city=city,
    ),
)

column_selection_task = PythonOperator(
    dag=dag,
    task_id='column_selection_task',
    provide_context=True,
    python_callable=column_selection,
    op_kwargs=dict(
        parquet_path=parquet_path,
        city=city,
    ),
)

check_if_file_exists_task \
    >> otodom_scraping_task \
    >> csv_dedup_and_to_parquet_task \
    >> lat_lon_cleanup_task \
    >> check_nullability_task \
    >> rough_join_with_map_data_task \
    >> column_renaming_task \
    >> column_selection_task

