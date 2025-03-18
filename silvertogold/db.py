import connection
import yaml
from sqlalchemy import text
from common_utils.logs import logger
from common_utils.audit import auditable
from common_utils.timing import time_this

class CallSP:
    def __init__(self):
        with open('config.yml', 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        self.engine_postgres = connection.new_db_connection('postgresql')

    @time_this
    @auditable
    def call_sp(self,row):
        """Execute a stored procedure."""
        sp_name = row.targetprocedurename
        schema_name = row.targetschemaname
        
        query = f"CALL {schema_name}.{sp_name}(:source_count, :update_count, :insert_count);"
        logger.info(f"Executing: {query}")

        with self.engine_postgres.connect() as conn:
            trans = conn.begin()  # Start transaction
            try:
                result=conn.execute(text(query),{"source_count": None, "update_count": None, "insert_count": None})  # Execute stored procedure
                trans.commit()  # Commit transaction

                # Fetch the OUT parameter values
                output_values = result.fetchone()
                
                if output_values:
                    source_count, update_count, insert_count = output_values
                else:
                    source_count, update_count, insert_count = 0, 0, 0  # Default values
                logger.info(
                    f"SP Executed: {schema_name}.{sp_name} - Source Count: {source_count}, "
                    f"Update Count: {update_count}, Insert Count: {insert_count}"
                )

                return source_count, update_count, insert_count  
            except Exception as e:
                trans.rollback()  # Rollback if error occurs
                logger.info(f"Error executing stored procedure {sp_name}: {e}")
                return None, None, None  # Return None in case of failure
            