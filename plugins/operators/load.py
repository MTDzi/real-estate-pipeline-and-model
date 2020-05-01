from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries


class _LoadOperator(BaseOperator):

    @apply_defaults
    def __init__(
            self,
            table_name='',
            redshift_conn_id='',
            aws_conn_id='',
            *args, **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.insert_query = SqlQueries.get_insert_query(table_name)
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id

    def execute(self, context):
        self.log.info('Starting...')
        self.redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('Done setting up a Redshift hook')

        self.log.info(f'Running the insert query:\n\n{self.insert_query}')
        self.redshift_hook.run(self.insert_query)
        self.log.info('\n\n...Done!\n\n')


class LoadDimensionOperator(_LoadOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(
            self,
            # table_name='',
            # redshift_conn_id='',
            # aws_conn_id='',
            *args, **kwargs,
    ):
        super().__init__(
            # table_name=table_name,
            # redshift_conn_id=redshift_conn_id,
            # aws_conn_id=aws_conn_id,
            *args, **kwargs
        )

    def execute(self, context):
        super().execute(context)


class LoadFactOperator(_LoadOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(
            self,
            # table_name='',
            # redshift_conn_id='',
            # aws_conn_id='',
            *args, **kwargs,
    ):
        super().__init__(
            # table_name=table_name,
            # redshift_conn_id=redshift_conn_id,
            # aws_conn_id=aws_conn_id,
            *args, **kwargs
        )

    def execute(self, context):
        super().execute(context)
