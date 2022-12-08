from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    
    ui_color = '#80BD9E'

    TRUNCATE_DIMENSION_SQL = """
        TRUNCATE TABLE {};
        """

    INSERT_DIMENSION_SQL = """
        INSERT INTO {} ({}) {};
        """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift',
                 dimension_table_name='',
                 dimension_insert_columns='',
                 dimension_insert_sql='',
                 truncate_table=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dimension_table_name = dimension_table_name
        self.dimension_insert_sql = dimension_insert_sql
        self.dimension_insert_columns = dimension_insert_columns
        self.truncate_table = truncate_table

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        if self.truncate_table:
            self.log.info(f"Truncating table {self.dimension_table_name}")
            redshift_hook.run(self.TRUNCATE_DIMENSION_SQL.format(self.dimension_table_name))    
        
        self.log.info(f"Inserting data into dimension table {self.dimension_table_name}")
        redshift_hook.run(self.INSERT_DIMENSION_SQL.format(
            self.dimension_table_name, 
            self.dimension_insert_columns,
            self.dimension_insert_sql))