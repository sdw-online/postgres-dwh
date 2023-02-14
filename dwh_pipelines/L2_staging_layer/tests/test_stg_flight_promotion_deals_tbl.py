import os
import re
import pytest 
import psycopg2
import configparser
from datetime import datetime




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

table_name                      =   'stg_flight_promotion_deals_tbl'
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
    sql_query = f"""     SELECT schema_name FROM information_schema.schemata 
    """
    cursor.execute(sql_query)

    sql_results = cursor.fetchall()
    schemas = [schema[0] for schema in sql_results]

    assert schema_name in schemas, f"The '{schema_name}' schema should be found in the '{database_name}' database. "






# ====================================== TEST 3: COLUMNS EXISTENCE CHECK ======================================

"""  Verify the columns of this table exists in the Postgres staging database   """



def test_columns_existence():
    sql_query = f"""     SELECT column_name FROM information_schema.columns WHERE table_name='{table_name}' 
    """
    cursor.execute(sql_query)

    sql_results = cursor.fetchall()
    actual_columns = [column[0] for column in sql_results]

    expected_columns = ['promotion_id',
                        'promotion_name',
                        'flight_booking_id',
                        'applied_discount',
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
    sql_query = f"""     SELECT * FROM information_schema.tables WHERE table_name = '{table_name}' AND table_schema = '{schema_name}'  ;  """
    cursor.execute(sql_query)
    sql_result  = cursor.fetchone()

    assert sql_result is not None, f"The '{table_name}' does not exist in the '{database}.{schema_name}' schema. "





# ====================================== TEST 5: DATA TYPES CHECK ======================================


""" Test if each column is mapped to the expected data type in Postgres  """


def test_column_data_types():

    # Create a dictionary that specifies the expected data types for each column  
    expected_data_types = {
        'promotion_id'                  :       "uuid",
        'promotion_name'                :       "character varying",
        'flight_booking_id'             :       "uuid",
        'applied_discount'              :       "numeric",
        "created_at"                    :       "timestamp with time zone",
        "updated_at"                    :       "timestamp with time zone",
        "source_system"                 :       "character varying",
        "source_file"                   :       "character varying",
        "load_timestamp"                :       "timestamp without time zone",
        "dwh_layer"                     :       "character varying"

    }   



    # Use SQL to extract the column names and their data types
    sql_query = f"""         SELECT column_name, data_type from information_schema.columns WHERE table_name = '{table_name}'
    """
    cursor.execute(sql_query)

    sql_results = cursor.fetchall()

    for column_name, actual_data_type in sql_results:
        assert actual_data_type.lower() == expected_data_types[column_name], f"The expected data type for column '{column_name}' was '{expected_data_types[column_name]}', but the actual data type was '{actual_data_type}'. "
    




# ====================================== TEST 6: EMPTY VALUES CHECK ======================================


""" Check if there are any empty values present in your table """

def test_empty_values_in_table():
    sql_query = f"""     SELECT * FROM   {schema_name}.{table_name}
    """
    cursor.execute(sql_query)
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







# ====================================== TEST 8: ID CHARACTER LENGTH CONSTRAINT CHECK ======================================

""" Test all the ID columns in the table contain 36 characters in length  """

def test_id_char_length_constraint():
    expected_id_char_length = 36
    sql_results = cursor.fetchall()
    

    sql_query = f"""     SELECT column_name FROM information_schema.columns WHERE table_name='{table_name} AND column_name LIKE "%_id%" ' 
    """
    cursor.execute(sql_query)
    
    sql_results = cursor.fetchall()


     # Assert the number of characters for the id column is equal to 36
    for sql_result in sql_results:
        id_column = sql_result[0]
        actual_id_length = len(id_column)
        assert actual_id_length == expected_id_char_length, f"Invalid ID column found: All ID columns must be {expected_id_char_length} characters long. The ID column containing invalid IDs is '{id_column}' column"




# ====================================== TEST 9: DUPLICATES CHECK ======================================


""" Test the number of duplicate records appearing in the Postgres table  """

def test_duplicate_records_count():
    column_name = "promotion_id"
    sql_query   = f"""                 SELECT          {column_name}, 
                                                        COUNT (*)
                                        FROM            {schema_name}.{table_name}
                                        GROUP BY        {column_name}
                                        HAVING          COUNT(*) > 1
                                        ;
    """
    cursor.execute(sql_query)

    duplicates = cursor.fetchall()
    total_no_of_duplicates = len(duplicates)
    
    # Assert the number of uniqueness constraints for the table specified is at least 1
    assert total_no_of_duplicates == 0, f"Duplicate entries detected - {table_name} should contain no duplicate entries."

