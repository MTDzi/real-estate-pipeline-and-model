from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(
            self,
            table_names=[''],
            redshift_conn_id='',
            *args, **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.table_names = table_names
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        self.redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for table_name in self.table_names:
            self._check_one(table_name)

    def _check_one(self, table_name):
        records = self.redshift_hook.get_records(f'SELECT COUNT(*) FROM {table_name}')
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f'Data quality check failed. {table_name} returned no results')
        num_records = records[0][0]
        if num_records < 1:
            raise ValueError(f'Data quality check failed. {table_name} contained 0 rows')
        self.log.info(f'Data quality on table {table_name} check passed with {records[0][0]} records')
