from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 table,
                 sql,
                 mode,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.mode = mode
    
    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.mode == 'insert':
            redshift.run(f'INSERT INTO {self.table} {self.sql}')
            self.log.info(f'{self.table} data is inserted')
        elif self.mode == 'delete':
            redshift.run(f'DELETE FROM {self.table}')
            self.log.info(f'{self.table} data is deleted')
        