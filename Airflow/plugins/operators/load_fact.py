from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    
    insert_sql = """
    INSERT INTO {} {} 
    {};
    """

    @apply_defaults
    def __init__(self,
                 table,
                 columns,
                 create_sql_query,
                 insert_sql_query,
                 redshift_conn_id="redshift",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.columns = columns
        self.create_sql_query = create_sql_query
        self.insert_sql_query = insert_sql_query

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        self.log.info('Droping table if exists...')
        # Here: drop the tables, execute the sql
        try:
            redshift.run(f"DROP TABLE IF EXISTS {self.table}")
        except Exception as e:
            print(e)
            self.log.warning(f"Something didn't work out out: {e}")
        
        self.log.info('Creating the table...')
        try:
            redshift.run(create_sql_query)
        except Exception as e:
            print(e)
            self.log.warning(f"Something didn't work out out: {e}")
    
        self.log.info('Inserting the data into the table...')
        try:
            insert_query = LoadFactOperator.insert_sql.format(
                self.table, 
                self.columns, 
                self.insert_sql_query
            )
            redshift.run(insert_query)
            self.log.info('Data has been inserted successfully.')
        except Exception as e:
            print(e)
            self.log.warning(f"Something didn't work out out: {e}")
