from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

# Fact operator will use SQL helper class to run data transformations.
# It will build songplays table
class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    
    insert_sql = """ INSERT INTO {table} {select_sql}; """

    @apply_defaults
    def __init__(self,
                 # Operators params (with defaults)
                 redshift_conn_id='redshift',
                 table='',
                 query='',
                 append_mode=False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id= redshift_conn_id
        self.table=table
        self.query=query
        self.append_mode=append_mode
       
    def execute(self, context):
        self.log.info('LoadFactOperator execution')
        redshift= PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.append_mode:
            self.log.info('Append mode, no delete rows and just appending data in {}'.format(self.table))
        else:
            self.log.info('Not Append mode, delete rows from table {}'.format(self.table))
            redshift.run('DELETE FROM {}'.format(self.table))
        
        self.log.info('Insert rows into table {}'.format(self.table))
        sql = LoadFactOperator.insert_sql.format(
            table=self.table,
            select_sql=self.query
        )
        
        redshift.run(sql)
        self.log.info('songplays table shall have data, please check!')
