from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    """
    Executes a COPY command to load files from S3 to Redshift

    (I borrowed a lot of code from airflow.providers.amazon.aws.operators.S3ToRedshiftTransfer
     but since it had some limitations, I had to make a few changes)
    """

    template_fields = ()
    template_ext = ()
    ui_color = '#358140'

    @apply_defaults
    def __init__(
            self,
            table_name,
            s3_url,
            redshift_conn_id,
            aws_conn_id,
            json_paths='auto',
            *args, **kwargs,
    ):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table_name = table_name
        self.s3_url = s3_url
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.json_paths = json_paths

    def execute(self, context):
        self.redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.s3 = S3Hook(aws_conn_id=self.aws_conn_id)
        credentials = self.s3.get_credentials()

        copy_query = self.get_copy_json_query(
            table_name=self.table_name,
            s3_url=self.s3_url,
            aws_key=credentials.access_key,
            aws_secret=credentials.secret_key,
            json_paths=self.json_paths,
        )

        self.log.info('Executing COPY command...')
        self.redshift_hook.run(copy_query)
        self.log.info('COPY command complete...')

    @staticmethod
    def get_copy_json_query(table_name, s3_url, aws_key, aws_secret, json_paths='auto'):
        return (
            f"COPY {table_name} FROM '{s3_url}'"
            " WITH CREDENTIALS"
            f" 'aws_access_key_id={aws_key};aws_secret_access_key={aws_secret}'"
            f" FORMAT AS JSON '{json_paths}';"
        )




