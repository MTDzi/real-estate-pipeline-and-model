from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator


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

city = 'Warszawa'
year_month_day = datetime.now().strftime('%Y%m%d')

dag = DAG(
    'scraping_dag',
    default_args=default_args,
    description='Scrape real estate data from otodom.pl',
    schedule_interval=None,
)

scrape_op = BashOperator(
    dag=dag,
    task_id='run_scrapy',
    bash_command=(
        'cd /usr/local/airflow/otodom_scraper/;'
        f' ../scraper_venv/bin/scrapy crawl otodomSpider -o ../scraped_csvs/{city}_{year_month_day}.csv -a city={city}'
        ' &> /dev/null'
    ),
)

scrape_op

