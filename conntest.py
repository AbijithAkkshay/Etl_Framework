import connection
from sqlalchemy import text

engine_postgres = connection.new_db_connection('postgresql')

with engine_postgres.connect() as conn:
    trans = conn.begin()  # Start transaction
    query="Select * from ods.d_clients"
    result=conn.execute(text(query))
    print(result)