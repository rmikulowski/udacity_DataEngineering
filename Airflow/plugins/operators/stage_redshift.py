from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    copy_sql = """
    COPY {}
    FROM '{}'
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    region 'us-west-2'
    FORMAT AS JSON 'auto'
    """

    @apply_defaults
    def __init__(self,
                 table="",
                 redshift_conn_id="redshift",
                 aws_credentials_id="aws_credentials",
                 create_sql_query="",
                 s3_bucket="udacity-dend",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table=table
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id=aws_credentials_id
        self.create_sql_query=create_sql_query
        self.s3_bucket=s3_bucket
        

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('Droping table if exists...')
        # Here: drop the tables, execute the sql
        try:
            redshift.run(f"DROP TABLE IF EXISTS {self.table}")
        except Exception as e:
            print(e)
            self.log.warning(f"Something didn't work out out: {e}")
            
        self.log.info('Creating table...')
        try:
            redshift.run(create_sql_query)
        except Exception as e:
            print(e)
            self.log.warning(f"Something didn't work out out: {e}")
        
        self.log.info('Staging the data...')
        
        facts_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            self.s3_bucket,
            credentials.access_key,
            credentials.secret_key
        )
        redshift.run(facts_sql)
        self.log.info('Data got into staging area.')





