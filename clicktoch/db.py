import time
import yaml
from sqlalchemy import text
from common_utils.audit import auditable
from common_utils.timing import time_this
import connection
from common_utils.logs import logger
import pandas as pd

class ClickToCH:
    def __init__(self):
        with open('config.yml', 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        self.config_stg = self.config['postgresql']['connection']
        self.engine_postgres = connection.new_db_connection('postgresql')
        self.engine_clickhouse = connection.new_db_connection('clickhouse-tcp')

    @time_this
    @auditable
    def click_to_ch(self, row):
        '''Move data from click schema to ClickHouse'''
        table_name = row.targetobject
        schema_name = row.sourceschema
        database_name = row.targetname
        # print(table_name,schema_name,database_name)
        incnt, upcnt, srcnt = 0, 0, 0

        with self.engine_postgres.connect() as con:
            res = con.execute(text(f"Select count(*) from {schema_name}.{table_name}"))
            res=res.fetchall()
            srcnt = res[0][0]
        
        if not self._table_exists(database_name, table_name):
            self._create_clickhouse_table(database_name, schema_name, table_name)
        
        if not self._columns_match(database_name, schema_name, table_name):
            self._recreate_table(database_name, schema_name, table_name)
        
        # if row.encrypted_columns:
        #     self._transfer_encrypted_data(row, database_name, schema_name, table_name)
        
        incnt = self._transfer_data(row, database_name, schema_name, table_name)
                   
        # time.sleep(0.5)
        return (srcnt,incnt,upcnt)

    def _table_exists(self, database_name, table_name):
        query = "SELECT count(*) FROM system.tables WHERE database = %(database_name)s AND name = %(table_name)s;"
        params = {'database_name': database_name, 'table_name': table_name}
        result = self.engine_clickhouse.execute(query, params)  # ClickHouse driver execution
        return result[0][0] != 0 

    def _create_clickhouse_table(self, database_name, schema_name, table_name):
        logger.error("Table %s.%s does not exist in ClickHouse", database_name, table_name)
        query = "select * from ods.clickhouse_table_script_generator(:target_database, :source_schema, :source_table)"
        params = {'target_database': database_name, 'source_schema': schema_name, 'source_table': table_name}
        with self.engine_postgres.connect() as conn:
            result = conn.execute(text(query), params)
            # print(result)
            create_table_query = result.first()[0]
        
        logger.info(create_table_query)
        self.engine_clickhouse.execute(create_table_query)
        logger.info("Created table %s.%s in ClickHouse", database_name, table_name)

    def _columns_match(self, database_name, schema_name, table_name):
        clickhouse_columns = self._get_clickhouse_columns(database_name, table_name)
        postgres_columns = self._get_postgres_columns(schema_name, table_name)
        return set(clickhouse_columns) == set(postgres_columns)

    def _get_clickhouse_columns(self, database_name, table_name):
        query = "SELECT name FROM system.columns WHERE database = %(database_name)s AND table = %(table_name)s;"
        params = {'database_name': database_name, 'table_name': table_name}
        result = self.engine_clickhouse.execute(query, params)  # Using clickhouse_driver
        return [row[0] for row in result]  # Extracting column names

    def _get_postgres_columns(self, schema_name, table_name):
        query = "SELECT column_name FROM information_schema.columns WHERE table_schema = :schema_name AND table_name = :table_name;"
        params = {'schema_name': schema_name, 'table_name': table_name}
        with self.engine_postgres.connect() as conn:
            result = conn.execute(text(query), params)
        return [row[0] for row in result.fetchall()]


    def _recreate_table(self, database_name, schema_name, table_name):
        logger.error("Columns in ClickHouse table %s and Postgres table %s are different", table_name, table_name)
        # Get the CREATE TABLE script from PostgreSQL
        query = "SELECT * FROM ods.clickhouse_table_script_generator(:target_database, :source_schema, :source_table)"
        params = {'target_database': database_name, 'source_schema': schema_name, 'source_table': table_name}
        with self.engine_postgres.connect() as conn:
            result = conn.execute(text(query), params)
            create_table_query = result.first()[0]  # Fetch the generated CREATE TABLE script
        # Drop existing ClickHouse table
        query_drop = f"DROP TABLE IF EXISTS {database_name}.{table_name};"
        logger.info(query_drop)
        self.engine_clickhouse.execute(query_drop)  # Using clickhouse_driver to drop the table
        # Create the new ClickHouse table
        logger.info(create_table_query)
        self.engine_clickhouse.execute(create_table_query)  # Using clickhouse_driver to execute the CREATE TABLE statement


    def _transfer_encrypted_data(self, row, database_name, schema_name, table_name):
        time_start = time.perf_counter()
        
        encryption_key = "12121112121212231123121212122321"
        cols = [
            f"pgp_sym_decrypt({col}::bytea, '{encryption_key}') as {col}" 
            if col in row.encrypted_columns.split(',') else col 
            for col in self._get_postgres_columns(schema_name, table_name)
        ]
        select_query = f"SELECT {', '.join(cols)} FROM {schema_name}.{table_name};"
        with self.engine_postgres.connect() as conn:
            result = conn.execute(text(select_query))
            rows = result.fetchall()

        row_count = len(rows)
        logger.info(f'{table_name.ljust(35)}{row_count:,} rows\t'+f'{time.perf_counter() - time_start:5.2f} seconds\t'+f'{table_name.ljust(39)}\tDB_READ')
        time_start = time.perf_counter()

        truncate_query = f"TRUNCATE TABLE {database_name}.{table_name};"
        self.engine_clickhouse.execute(truncate_query)
        # Insert data into ClickHouse
        insert_query = f"INSERT INTO {database_name}.{table_name} VALUES"      
        # Convert rows to list of tuples
        data_to_insert = [tuple(row) for row in rows]
        # Execute batch insert
        self.engine_clickhouse.execute(insert_query, data_to_insert)
        
        logger.info(f'{table_name.ljust(35)}{row_count:,} rows\t'+f'{time.perf_counter() - time_start:5.2f} seconds\t'+f'{table_name.ljust(39)}\tDB_WRITE')


    def _transfer_data(self, row, database_name, schema_name, table_name):
        time_start = time.perf_counter()

        # PostgreSQL query to fetch data
        select_query = f"SELECT * FROM {schema_name}.{table_name};"

        # Fetch data from PostgreSQL using SQLAlchemy
        with self.engine_postgres.connect() as pg_conn:
            df = pd.read_sql(select_query, pg_conn)  # Read into DataFrame
            # print(df)
        # Truncate ClickHouse table before inserting new data
        self.engine_clickhouse.execute(f"TRUNCATE TABLE {database_name}.{table_name};")

        # Convert DataFrame to list of tuples
        data_tuples = [tuple(row) for row in df.itertuples(index=False, name=None)]

        # Insert data in ClickHouse using batch insert
        insert_query = f"INSERT INTO {database_name}.{table_name} ({', '.join(df.columns)}) VALUES"

        # Execute batch insert
        self.engine_clickhouse.execute(insert_query, data_tuples)

        # Get row count
        row_count = self.engine_clickhouse.execute(f"SELECT count(*) FROM {database_name}.{table_name};")[0][0]

        logger.info(f'{table_name.ljust(35)}{row_count:,} rows\t' +
                    f'{time.perf_counter() - time_start:5.2f} seconds\t' +
                    f'{table_name.ljust(39)}\tDB_WRITE')
        
        return row_count

