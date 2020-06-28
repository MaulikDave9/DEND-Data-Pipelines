import datetime
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.S3_hook import S3Hook
from airflow.contrib.hooks.aws_hook import AwsHook

# Stage operator will load any JSON formatted files from S3 to Amazon Redshift
# it will create and run SQL COPY statement based on parameters. 

# Reference: https://knowledge.udacity.com/questions/204454

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ('s3_key','execution_date')
    
    # SQL template for csv input format
    copy_sql_csv = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}' 
        IGNOREHEADER {}
        DELIMITER    '{}'
        REGION 'us-west-2'
        COMPUDATE OFF;
    """
    
    # SQL template for JSON input format
    copy_sql_json = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}' 
        REGION 'us-west-2'
        JSON '{}'
        COMPUDATE OFF;
    """

    @apply_defaults
    def __init__(self,
                 # Oerators params (with defaults):
                 aws_conn_id          = 'aws_credentials',
                 redshift_conn_id     = 'redshift',
                 table                = '',
                 s3_bucket            = '',
                 s3_key               = '',
                 file_type            = '',
                 json_paths           = '',
                 ignore_header        = 1,
                 delimiter            = ',',
                 execution_date       = '',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params:
        self.aws_conn_id       = aws_conn_id
        self.redshift_conn_id  = redshift_conn_id
        self.table             = table
        self.s3_bucket         = s3_bucket
        self.s3_key            = s3_key
        self.file_type         = file_type
        self.json_paths        = json_paths
        self.ignore_header     = ignore_header
        self.delimiter         = delimiter
        self.execution_date    = execution_date  

    def execute(self, context):
        
        # logging different steps of execution
        # SQL statements are executed by using a Airflow hook
        # s3_key="log_data/{execution_date.year}/{execution_date.month:02}/{ds}-events.json" 
        
        self.log.info('StageToRedshiftOperator execution')
        aws_hook         = AwsHook(self.aws_conn_id)
        credentials      = aws_hook.get_credentials()
        redshift         = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('Created Redshift connection')
       
        self.log.info('Delete rows from table {}'.format(self.table))
        redshift.run('DELETE FROM {}'.format(self.table))
                      
        self.log.info('Copy data from S3 to Redshift')
        #rendered_date        = self.execution_date.format(**context)
        #rendered_date_object = datetime.datetime.strptime(rendered_date,'%Y-%m-%d')
        
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        
        if self.json_paths == '':
            s3_json_path = 'auto'
        else:
            s3_json_path = "s3://{}/{}".format(self.s3_bucket, self.json_paths)
        
        self.log.info('s3_path: {}'.format(s3_path))
        self.log.info('s3_json_path: {}'.format(s3_json_path))
        
        # Assumption: only JSON and CSV file types.
        if self.file_type == 'JSON':
            formatted_sql = StageToRedshiftOperator.copy_sql_json.format(
                self.table,
                credentials.access_key,
                credentials.secret_key,
                s3_json_path
            )
        else: 
            formatted_sql = StageToRedshiftOperator.copy_sql_csv.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.ignore_header,
                self.delimiter
            )
        redshift.run(formatted_sql)
        
        self.log.info('WOW! Redshift Copy was success!!')