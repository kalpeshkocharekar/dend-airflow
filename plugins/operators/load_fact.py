from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    query = """
        BEGIN;
        DROP TABLE IF EXISTS {};
        {};
        INSERT INTO {}
        {};
        COMMIT;
    """
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 redshift_iam_role='',
                 table='',
                 create_query='',
                 insert_query='', # Query to run to insert to the table.
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.redshift_iam_role = redshift_iam_role
        self.table = table
        self.create_query = create_query
        self.insert_query = insert_query

    def execute(self, context):
        # Hook to access Amazon RedShift for the copy operation.
        redshift_hook = PostgresHook(self.redshift_conn_id)
        # Recreate table.
        redshift_hook.run(LoadFactOperator.query.format(
            self.table,
            self.create_query,
            self.table,
            self.insert_query
        ))
