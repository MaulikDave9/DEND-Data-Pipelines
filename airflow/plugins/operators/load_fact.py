from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    
    insert_sql = """ INSERT INTO {{table}} {select_sql}; """

    @apply_defaults
    def __init__(self,
                 # Operators params (with defaults)
                 redshift_conn_id='redshift',
                 table='',
                 query='',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id= redshift_conn_id,
        self.table=table
        self.query=query
       
    def execute(self, context):
        self.log.info('LoadFactOperator execution')
        redshift= PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('Created Redshift connection')
        
        self.log.info('Delete rows from table {}'.format(self.table))
        redshift.run('DELETE FROM {}'.format(self.table))
        
        self.log.info('INSERT INTO TABLE {}'.format(self.table))
        sql = LoadFactOperator.insert_sql.format(
            table=self.table,
            select_sql=self.query)
        
        redshift.run(sql)
        
