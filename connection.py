# import yaml
# import psycopg2
# import psycopg2.extensions
# import clickhouse_driver
# from sqlalchemy import create_engine, engine
# from urllib.parse import quote_plus

# # Load configuration file
# with open("config.yml", "r") as f:
#     config = yaml.safe_load(f)

# # Function to create a new database connection
# def new_db_connection(location: str = "", use_sqlalchemy: bool = True):
#     if location not in config:
#         raise KeyError(f"Invalid location '{location}' in config file")

#     conn_details = config[location]["connection"]
#     dialect = config[location]["dialect"]
#     driver = config[location].get("driver", "")

#     # PostgreSQL Connection
#     if dialect.startswith("postgresql"):
#         if use_sqlalchemy:
#             conn_details["password"] = quote_plus(conn_details["password"])
#             conn_str = "{dialect}://{username}:{password}@{host}:{port}/{database}".format(**conn_details, dialect=dialect)
#             return create_engine(conn_str)
#         else:
#             return psycopg2.connect(
#                 host=conn_details["host"],
#                 dbname=conn_details["database"],
#                 user=conn_details["username"],
#                 password=conn_details["password"],
#                 port=conn_details["port"]
#             )

#     # ClickHouse Connection
#     elif dialect.startswith("clickhouse"):
#         return clickhouse_driver.connect(
#             host=conn_details["host"],
#             port=conn_details["port"],
#             database=conn_details["database"],
#             user=conn_details["username"],
#             password=conn_details["password"],
#         )

#     else:
#         raise ValueError(f"Unsupported dialect: {dialect}")




import yaml
import psycopg2
import psycopg2.extensions
import clickhouse_driver
from sqlalchemy import create_engine
from urllib.parse import quote_plus

# Load configuration file
with open("config.yml", "r") as f:
    config = yaml.safe_load(f)

# Function to create a new database connection
def new_db_connection(location: str = "", use_sqlalchemy: bool = True):
    if location not in config:
        raise KeyError(f"Invalid location '{location}' in config file. Available options: {list(config.keys())}")

    conn_details = config[location].get("connection", {})
    dialect = config[location].get("dialect", "").lower()
    driver = config[location].get("driver", "")

    # Ensure required fields exist
    required_keys = ["host", "database", "username", "password", "port"]
    missing_keys = [key for key in required_keys if key not in conn_details]
    if missing_keys:
        raise KeyError(f"Missing keys in '{location}' config: {missing_keys}")

    # Encode password for SQLAlchemy connections
    password = conn_details["password"]
    if password:
        password = quote_plus(password)

    # PostgreSQL Connection
    if dialect.startswith("postgresql"):
        if use_sqlalchemy:
            conn_str = f"{dialect}://{conn_details['username']}:{password}@{conn_details['host']}:{conn_details['port']}/{conn_details['database']}"
            return create_engine(conn_str)
        else:
            return psycopg2.connect(
                host=conn_details["host"],
                dbname=conn_details["database"],
                user=conn_details["username"],
                password=conn_details["password"],
                port=conn_details["port"]
            )

    # ClickHouse Connection
    elif dialect.startswith("clickhouse"):
        
        return clickhouse_driver.Client(
                host=conn_details["host"],
                port=conn_details["port"],
                database=conn_details["database"],
                user=conn_details.get("username", ""),
                password=conn_details.get("password", ""),
        )

    else:
        raise ValueError(f"Unsupported dialect: {dialect}. Expected 'postgresql' or 'clickhouse'.")
