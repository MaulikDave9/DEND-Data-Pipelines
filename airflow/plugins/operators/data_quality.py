import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

# This will run checks on the data - test result and expected results
class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'
    count_rows= """ SELECT COUNT(*) FROM {table};"""

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift',
                 tables_list=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.tables_list=tables_list
        
    def execute(self, context):
        self.log.info('DataQualityOperator executing')
        redshift= PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        for table in self.tables_list:
            self.log.info('For Table: {}'.format(table))
            
            # Based on Exercise 4, Lecture 2 - Data Quality
            sql_statement = DataQualityOperator.count_rows.format(table= table)
            records = redshift.get_records(sql_statement)
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f'DQ Check alert!!! No results for the {table}')
            
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f'DQ Check alert!! No results for the {table}')
            self.log.info(f"Data quality on table {table} check passed with {records[0][0]} records")