from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 sql,
                 tables,
                 expect_values,
                 mode,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.tables = tables
        self.expect_values = expect_values
        self.mode = mode
        
    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for table, expect_value in zip(self.tables, self.expect_values):
            sql = self.sql.format(table)
            records = redshift.get_records(sql)
            if records is None:
                raise ValueError(f'{tablee} fail to pass the data quality test.')
            elif self.mode == 'equal' and records[0][0] != expect_value:
                raise ValueError(f'{tablee} fail to pass the data quality test.')
            elif self.mode =='not_equal' and records[0][0] == expect_value:
                raise ValueError(f'{tablee} fail to pass the data quality test.')
        self.log.info('data quality check is finished.')
        