from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 tables = [],
                 checks = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.tables=tables
        self.checks=checks

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        for table in self.tables:
            for check in self.checks:
                records = redshift.get_records(check['test_sql'].format(table))
                if check['equal'] and check['greater']:
                    result = records >= check['expected_result']
                elif check['equal'] and not check['greater']:
                    result = records <= check['expected_result']
                elif not check['equal'] and check['greater']:
                    result = records > check['expected_result']
                elif not check['equal'] and not check['greater']:
                    result = records < check['expected_result']
                elif check['equal'] and check['greater']=='':
                    result = records = check['expected_result']
                else:
                    self.log.error(f"Badly defined test with sql statement {check['test_sql']}.")
                
                if result==True:
                    self.log.info(f"Test with sql statement {check['test_sql']} was successful")
                else:
                    self.log.error(f"Error: sql statement {check['test_sql']} returned unexpected result.")
                   
