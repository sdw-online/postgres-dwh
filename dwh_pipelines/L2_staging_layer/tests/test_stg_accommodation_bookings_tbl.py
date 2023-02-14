import pytest 
import psycopg2
import configparser
import os
from datetime import datetime, timedelta, date
import re




# ================================================ CONFIG ================================================

# Add a flag/switch indicating whether Airflow is in use or not 
USING_AIRFLOW   =   False



# Create a config file for storing environment variables
config  =   configparser.ConfigParser()
if USING_AIRFLOW:

    # Use the airflow config file from the airflow container 
    config.read('/usr/local/airflow/dags/etl_to_postgres/airflow_config.ini')
    DATASETS_LOCATION_PATH = config['postgres_airflow_config']['DATASET_SOURCE_PATH'] 

    host                    =   config['postgres_airflow_config']['HOST']
    port                    =   config['postgres_airflow_config']['PORT']
    database                =   config['postgres_airflow_config']['STAGING_DB']
    username                =   config['postgres_airflow_config']['USERNAME']
    password                =   config['postgres_airflow_config']['PASSWORD']
    
    postgres_connection     =   None
    cursor                  =   None

    
else:

    # Use the local config file from the local machine 
    path    =   os.path.abspath('dwh_pipelines/local_config.ini')
    config.read(path)
    DATASETS_LOCATION_PATH     =   config['travel_data_filepath']['DATASETS_LOCATION_PATH']

    host                    =   config['travel_data_filepath']['HOST']
    port                    =   config['travel_data_filepath']['PORT']
    database                =   config['travel_data_filepath']['STAGING_DB']
    username                =   config['travel_data_filepath']['USERNAME']
    password                =   config['travel_data_filepath']['PASSWORD']

    postgres_connection     =   None
    cursor                  =   None



# Connect to the Postgres database
try:
    pgsql_connection = psycopg2.connect(
            host = host,
            port = port,
            dbname = database,
            user = username,
            password = password,
        )


    # Create a cursor object to execute the PG-SQL commands 
    cursor = pgsql_connection.cursor()


except psycopg2.Error:
    raise ConnectionError("CONNECTION ERROR: Unable to connect to the demo_company database...")



# Define the database, schema and table names

table_name                      =   'stg_accommodation_bookings_tbl'
schema_name                     =   'dev'
database_name                   =    database



# ====================================== TEST 1: DATABASE CONNECTION CHECK ======================================


""" Test the connection to the Postgres database is successful or not """

def test_database_connection():

    # Assert the existence of a valid connection to the database (i.e. not None) 
    assert pgsql_connection is not None, f"CONNECTION ERROR: Unable to connect to the {database_name} database... " 






# ====================================== TEST 2: SCHEMA EXISTENCE CHECK ======================================


"""  Verify the staging schema exists in the Postgres staging database   """



def test_schema_existence():

    cursor.execute(f"""     SELECT schema_name FROM information_schema.schemata
        
    """)

    sql_results = cursor.fetchall()
    schemas = [schema[0] for schema in sql_results]

    assert schema_name in schemas, f"The '{schema_name}' schema should be found in the '{database_name}' database. "






# ====================================== TEST 3: COLUMNS EXISTENCE CHECK ======================================

"""  Verify the columns of this table exists in the Postgres staging database   """



def test_columns_existence():

    cursor.execute(f"""     SELECT column_name FROM information_schema.columns WHERE table_name='{table_name}'
        
    """)

    sql_results = cursor.fetchall()
    actual_columns = [column[0] for column in sql_results]

    expected_columns = ['id', 
                        'booking_date', 
                        'check_in_date', 
                        'check_out_date', 
                        'checked_in',
                        'confirmation_code', 
                        'customer_id', 
                        'flight_booking_id', 
                        'location',
                        'no_of_adults', 
                        'no_of_children', 
                        'payment_method', 
                        'room_type',
                        'sales_agent_id', 
                        'status', 
                        'total_price',
                        'created_at',
                        'updated_at',
                        'source_system',
                        'source_file',
                        'load_timestamp',
                        'dwh_layer'
                        ]
    
    for expected_column in expected_columns:
        assert expected_column in actual_columns, f"The '{expected_column}' column should be in the '{table_name}' table. "





# ====================================== TEST 4: TABLE EXISTENCE CHECK ======================================


""" Check if the active table is in the Postgres staging database  """


def test_table_existence():
    cursor.execute(f"""     SELECT * FROM information_schema.tables WHERE table_name = '{table_name}' AND table_schema = '{schema_name}'  ;  """)
    sql_result  = cursor.fetchone()

    assert sql_result is not None, f"The '{table_name}' does not exist in the '{database}.{schema_name}' schema. "





# ====================================== TEST 5: DATA TYPES CHECK ======================================


""" Test if each column is mapped to the expected data type in Postgres  """


def test_column_data_types():

    # Create a dictionary that specifies the expected data types for each column  
    expected_data_types = {
        "id":                   "uuid",
        "booking_date":         "date",
        "check_in_date":        "date",
        "check_out_date":       "date",
        "checked_in":           "character varying",
        "confirmation_code":    "character varying",
        "customer_id":          "uuid",
        "flight_booking_id":    "uuid",
        "location":             "character varying",
        "no_of_adults":         "integer",
        "no_of_children":       "integer",
        "payment_method":       "character varying",
        "room_type":            "character varying",
        "sales_agent_id":       "uuid",
        "status":               "character varying",
        "total_price":          "numeric",
        "created_at":           "timestamp with time zone",
        "updated_at":           "timestamp with time zone",
        "source_system":        "character varying",
        "source_file":          "character varying",
        "load_timestamp":       "timestamp without time zone",
        "dwh_layer":            "character varying"

    }   



    # Use SQL to extract the column names and their data types
    cursor.execute(f"""         SELECT column_name, data_type from information_schema.columns WHERE table_name = '{table_name}'
    
    """)

    sql_results = cursor.fetchall()

    for column_name, actual_data_type in sql_results:
        assert actual_data_type.lower() == expected_data_types[column_name], f"The expected data type for column '{column_name}' was '{expected_data_types[column_name]}', but the actual data type was '{actual_data_type}'. "
    




# ====================================== TEST 6: EMPTY VALUES CHECK ======================================


""" Check if there are any empty values present in your table """

def test_empty_values_in_table():
    cursor.execute(f"""     SELECT * FROM   {schema_name}.{table_name}
    """)
    sql_results = cursor.fetchall()

    row_no = 0 
    for record in sql_results:
        row_no +=1
        for cell_value in record:
            assert cell_value is not None, f" There is an empty value in the '{schema_name}.{table_name}' table on row '{row_no}' . "






# ====================================== TEST 7: NULL VALUES CHECK ======================================

""" Check if there are any NULL values present in your table """

def test_null_values_in_table():

    # Get list of columns from table 
    cursor.execute(f""" SELECT column_name from information_schema.columns WHERE table_name = '{table_name}' ;
    """)
    columns = cursor.fetchall()


    for column in columns:
        sql_query = f'SELECT COUNT(*) FROM {schema_name}.{table_name} WHERE {column[0]} is NULL'
        cursor.execute(sql_query)
        sql_result = cursor.fetchone()

        assert sql_result[0] == 0, f"The {column} column has NULL values. "







# ====================================== TEST 8: ROW COUNT CHECK ======================================