from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

# This will be used to build four dimension tables - users, songs, artists, time 
class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    insert_sql = """INSERT INTO {table} {select_sql};"""

    @apply_defaults
    def __init__(self,
                 # Operators params (with defaults)
                 redshift_conn_id='redshift',
                 table='',
                 query='',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table            = table
        self.query            = query

    def execute(self, context):
        self.log.info('LoadDimensionOperator execution')
        redshift= PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info('Delete rows from table {}'.format(self.table))
        redshift.run('DELETE FROM {}'.format(self.table))
        
        self.log.info('Insert rows into table {}'.format(self.table))
        sql = LoadDimensionOperator.insert_sql.format(
            table=self.table,
            select_sql=self.query
        )
        
        redshift.run(sql)
        self.log.info('{} table shall have data, please check!'.format(self.table))