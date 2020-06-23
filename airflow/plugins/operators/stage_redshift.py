from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.S3_hook import S3Hook
from airflow.contrib.hooks.aws_hook import AwsHook

# Stage operator will load any JSON formatted files from S3 to Amazon Redshift
# it will create and run SQL COPY statement based on parameters. 

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    # SQL template for JSON input format
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        IGNOREHEADER {}
        REGION 'us-west-2'
        JSON '{}'
    """

    @apply_defaults
    def __init__(self,
                 # Oerators params (with defaults):
                 aws_conn_id          = 'aws_credentials',
                 redshift_conn_id     = 'redshift',
                 table                = 'staging_table_name',
                 s3_bucket            = '',
                 s3_key               = '',
                 json_path            = 'auto',
                 ignore_headers       = 1,
                 count_query          = """SELECT COUNT(*) FROM table_name;""",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params:
        self.aws_conn_id       = aws_conn_id
        self.redshift_conn_id  = redshift_conn_id
        self.ignore_headers    = ignore_headers
        self.table             = table
        self.s3_bucket         = s3_bucket
        self.s3_key            = s3_key
        self.json_path         = json_path
        self.count_query       = count_query

    def execute(self, context):
        self.log.info('StageToRedshiftOperator execution')
        aws_hook         = AwsHook(self.aws_conn_id)
        credentials      = aws_hook.get_credentials()
        redshift         = PostgresHook(postgres_conn_id=self.redshift_conn_id)
       
        self.log.info('Delete rows from table {}'.format(self.table))
        redshift.run('DELETE FROM {}'.format(self.table))
                      
        self.log.info('Copy data from S3 to Redshift')
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        if self.json_path != 'auto':
             json_path = "s3://{}/{}".format(self.s3_bucket, self.json_path)
        else:
             json_path = self.json_path
        
        formatted_sql = StageToRedShiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.ignore_headers,
            json_path)
        redshift.run(formatted_sql)