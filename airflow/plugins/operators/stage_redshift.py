from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.S3_hook import S3Hook
from airflow.contrib.hooks.aws_hook import AwsHook


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    # SQL template for JSON input format
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION 'us-west-2'
        JSON '{}'
    """

    @apply_defaults
    def __init__(self,
                 # Oerators params (with defaults):
                 aws_conn_id    = 'aws_credentials',
                 db_conn_id     = 'redshift',
                 table          = 'staging_table_name',
                 data_path      = '',
                 jsonpaths_path = 'auto',
                 count_query    = """SELECT COUNT(*) FROM table_name;""",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params:
        self.aws_conn_id     = aws_conn_id
        self.db_conn_id      = db_conn_id
        self.table           = table
        self.data_path       = data_path
        self.jsonpaths_path  = jsonpaths_path
        self.count_query     = count_query

    def execute(self, context):
        self.log.info('StageToRedshiftOperator not implemented yet')





