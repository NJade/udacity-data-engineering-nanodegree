from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 aws_credentials_id,
                 redshift_conn_id,
                 sql,
                 table,
                 s3_bucket,
                 s3_directory,
                 json_format,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_directory = s3_directory
        self.json_format = json_format
        

    def execute(self, context):
        aws_hook = AwsHook(aws_conn_id=self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        redshift.run(f'DELETE FROM {self.table}')
        self.log.info(f'{self.table} data is deleted')
        
        s3_path = f's3://{self.s3_bucket}/{self.s3_directory}'
        sql = self.sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json_format
        )
        redshift.run(sql)
        self.log.info(f'{self.table} data is inserted')
