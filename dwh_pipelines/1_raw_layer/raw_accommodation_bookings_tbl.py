import psycopg2
import json
import configparser
import logging, coloredlogs
import pandas as pd 
from datetime import datetime
import os 
from pathlib import Path
import time 



# ================================================ LOGGER ================================================

# Set up root root_logger 
root_logger = logging.getLogger(__name__)
root_logger.setLevel(logging.DEBUG)

# Add colour to the console prints 
coloredlogs.install(level='DEBUG', logger=root_logger, fmt='%(message)s')

# Set up formatter for logs 
file_handler_log_formatter = logging.Formatter('%(asctime)s  |  %(levelname)s  |  %(message)s  ')
console_handler_log_formatter = coloredlogs.ColoredFormatter(fmt='%(message)s', level_styles=dict(
                                                                                        debug=dict(color='green'),
                                                                                        info=dict(color='blue'),
                                                                                        warning=dict(color='orange'),
                                                                                        error=dict(color='red', bold=True, bright=True),
                                                                                        critical=dict(color='black', bold=True, background='red')
                                                                                            ),
                                                                                    field_styles=dict(
                                                                                        messages=dict(color='white')
                                                                                    ))

# Set up file handler object for logging events to file
current_filepath = Path(__file__).stem
file_handler = logging.FileHandler('logs/raw_layer/' + current_filepath + '.log', mode='w')
file_handler.setFormatter(file_handler_log_formatter)

# Set up console handler object for writing event logs to console in real time (i.e. streams events to stderr)
console_handler = logging.StreamHandler()
console_handler.setFormatter(console_handler_log_formatter)



# Add the file and console handlers 
root_logger.addHandler(file_handler)
root_logger.addHandler(console_handler)










# ================================================ CONFIG ================================================

# Add a flag/switch indicating whether Airflow is in use or not 
USING_AIRFLOW = False

# Create source file variable 
src_file = 'accommodation_bookings.json'


# Create a config file for storing environment variables
config = configparser.ConfigParser()
if USING_AIRFLOW:

    # Use the airflow config file from the airflow container 
    config.read('/usr/local/airflow/dags/etl_to_postgres/airflow_config.ini')
    accommodation_bookings_path = config['postgres_airflow_config']['DATASET_SOURCE_PATH'] + src_file

    host                = config['postgres_airflow_config']['HOST']
    port                = config['postgres_airflow_config']['PORT']
    database            = config['postgres_airflow_config']['RAW_DB']
    username            = config['postgres_airflow_config']['USERNAME']
    password            = config['postgres_airflow_config']['PASSWORD']
    
    postgres_connection     = None
    cursor                  = None

    
else:

    # Use the local config file from the local machine 
    path = os.path.abspath('dwh_pipelines/local_config.ini')
    config.read(path)
    accommodation_bookings_path = config['travel_data_filepath']['DATASETS_LOCATION_PATH'] + "accommodation_bookings.json"

    host                = config['travel_data_filepath']['HOST']
    port                = config['travel_data_filepath']['PORT']
    database            = config['travel_data_filepath']['RAW_DB']
    username            = config['travel_data_filepath']['USERNAME']
    password            = config['travel_data_filepath']['PASSWORD']

    postgres_connection     = None
    cursor                  = None



# Begin the data extraction process
root_logger.info("")
root_logger.info("---------------------------------------------")
root_logger.info("Beginning the source data extraction process...")
extraction_start_time = time.time()


with open(accommodation_bookings_path, 'r') as accommodation_bookings_file:    
    
    try:
        accommodation_bookings_data = json.load(accommodation_bookings_file)
        root_logger.info(f"Successfully located '{src_file}'")
    # accommodation_bookings_data = accommodation_bookings_data[0:100]

    except:
        root_logger.error("Unable to locate source file...terminating process...")
        raise Exception("No source file located")
    

postgres_connection = psycopg2.connect(
            host = host,
            port = port,
            dbname = database,
            user = username,
            password = password,
        )


def load_data_to_raw_layer(postgres_connection):
    try:
        
        # Set up constants
        row_counter = 0 
        successful_rows_upload_count  =   0 
        failed_rows_upload_count      =   0 

        db_layer_name = database
        schema_name = 'main'
        table_name = 'raw_accommodation_bookings_tbl'


        # Create a cursor object to execute the PG-SQL commands 
        cursor = postgres_connection.cursor()



        # Validate the Postgres database connection
        if postgres_connection.closed == 0:
            root_logger.debug(f"")
            root_logger.info("=================================================================================")
            root_logger.info(f"CONNECTION SUCCESS: Managed to connect successfully to the {db_layer_name} database!!")
            root_logger.info(f"Connection details: {postgres_connection.dsn} ")
            root_logger.info("=================================================================================")
            root_logger.debug("")
        
        elif postgres_connection.closed != 0:
            raise ConnectionError("CONNECTION ERROR: Unable to connect to the demo_company database...") 
        




        # ======================================= LOAD SRC TO RAW =======================================
        




        # Set up SQL statements for schema creation and validation check  
        create_schema = f'''    CREATE SCHEMA IF NOT EXISTS {schema_name};
        '''

        check_if_schema_exists = f'''   SELECT schema_name from information_schema.schemata WHERE schema_name= '{schema_name}';
        '''


        # Set up SQL statements for table deletion and validation check  
        delete_raw_accommodation_bookings_tbl_if_exists = f''' DROP TABLE IF EXISTS {schema_name}.{table_name} CASCADE;
        '''

        check_if_raw_accommodation_bookings_tbl_is_deleted = f'''   SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}' );
        '''

        # Set up SQL statements for table creation and validation check 
        create_raw_accommodation_bookings_tbl = f'''                CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
                                                                            id                      UUID PRIMARY KEY,
                                                                            booking_date            TIMESTAMP,
                                                                            check_in_date           TIMESTAMP,
                                                                            check_out_date          TIMESTAMP,
                                                                            checked_in              VARCHAR(3),
                                                                            confirmation_code       VARCHAR(12),
                                                                            customer_id             UUID,
                                                                            flight_booking_id       UUID,
                                                                            location                TEXT,
                                                                            num_adults              INTEGER,
                                                                            num_children            INTEGER,
                                                                            payment_method          VARCHAR(20),
                                                                            room_type               VARCHAR(10),
                                                                            sales_agent_id          UUID,
                                                                            status                  VARCHAR(10),
                                                                            total_price             NUMERIC(18, 6)
                                                                        );



        '''

        check_if_raw_accommodation_bookings_tbl_exists = f'''       SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}' );
        '''



        # Set up SQL statements for adding data lineage and validation check 
        add_data_lineage_to_raw_accommodation_bookings_tbl = f'''        ALTER TABLE {schema_name}.{table_name}
                                                                            ADD COLUMN  created_at                  TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                                                                            ADD COLUMN  updated_at                  TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                                                                            ADD COLUMN  source_system               VARCHAR(255),
                                                                            ADD COLUMN  source_file                 VARCHAR(255),
                                                                            ADD COLUMN  load_timestamp              TIMESTAMP,
                                                                            ADD COLUMN  transformation_process      VARCHAR(255)
                                                                        ;
        '''

        check_if_data_lineage_fields_are_added_to_tbl = f'''        
                                                                    SELECT * 
                                                                    FROM    information_schema.columns 
                                                                    WHERE   table_name      = '{table_name}' 
                                                                        AND     (column_name    = 'created_at'
                                                                        OR      column_name     = 'updated_at' 
                                                                        OR      column_name     = 'source_system' 
                                                                        OR      column_name     = 'source_file' 
                                                                        OR      column_name     = 'load_timestamp' 
                                                                        OR      column_name     = 'transformation_process');
                                                                              
        '''

        insert_accommodation_bookings_data = f'''                       INSERT INTO {schema_name}.{table_name} (
                                                                                id, 
                                                                                booking_date, 
                                                                                check_in_date, 
                                                                                check_out_date, 
                                                                                checked_in, 
                                                                                confirmation_code, 
                                                                                customer_id, 
                                                                                flight_booking_id, 
                                                                                location, 
                                                                                num_adults, 
                                                                                num_children, 
                                                                                payment_method, 
                                                                                room_type, 
                                                                                sales_agent_id, 
                                                                                status, 
                                                                                total_price, 
                                                                                created_at, 
                                                                                updated_at, 
                                                                                source_system, 
                                                                                source_file, 
                                                                                load_timestamp, 
                                                                                transformation_process
                                                                                )

                                                                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s)
                                                                            ;
        '''

        check_if_rows_are_inserted_into_raw_accommodation_bookings_tbl = f'''
        '''


        # Create schema in Postgres
        cursor.execute(create_schema)
        cursor.execute(check_if_schema_exists)
        sql_result = cursor.fetchone()[0]
        root_logger.info(sql_result)
        if sql_result:
            root_logger.debug(f"")
            root_logger.info(f"=================================================================================================")
            root_logger.info(f"SCHEMA CREATION SUCCESS: Managed to create {schema_name} schema in {db_layer_name} ")
            root_logger.info(f"Schema name in Postgres: {sql_result} ")
            root_logger.info(f"SQL Query for validation check:  {check_if_schema_exists} ")
            root_logger.info(f"=================================================================================================")
            root_logger.debug(f"")

        else:
            root_logger.debug(f"")
            root_logger.error(f"=================================================================================================")
            root_logger.error(f"SCHEMA CREATION FAILURE: Unable to create schema for {db_layer_name}...")
            root_logger.info(f"SQL Query for validation check:  {check_if_schema_exists} ")
            root_logger.error(f"=================================================================================================")
            root_logger.debug(f"")

        

        # Delete table if it exists in Postgres
        cursor.execute(delete_raw_accommodation_bookings_tbl_if_exists)
        cursor.execute(check_if_raw_accommodation_bookings_tbl_is_deleted)
        sql_result = cursor.fetchone()[0]
        root_logger.info(sql_result)
        if sql_result:
            root_logger.debug(f"")
            root_logger.info(f"=============================================================================================================================================================================")
            root_logger.info(f"TABLE DELETION SUCCESS: Managed to drop {table_name} table in {db_layer_name}. Now advancing to recreating table... ")
            root_logger.info(f"SQL Query for validation check:  {check_if_raw_accommodation_bookings_tbl_is_deleted} ")
            root_logger.info(f"=============================================================================================================================================================================")
            root_logger.debug(f"")
        else:
            root_logger.debug(f"")
            root_logger.error(f"==========================================================================================================================================================================")
            root_logger.error(f"TABLE DELETION FAILURE: Unable to delete {table_name}. This table may have objects that depend on it (use DROP TABLE ... CASCADE to resolve) or it doesn't exist. ")
            root_logger.error(f"SQL Query for validation check:  {check_if_raw_accommodation_bookings_tbl_is_deleted} ")
            root_logger.error(f"==========================================================================================================================================================================")
            root_logger.debug(f"")



        # Create table if it doesn't exist in Postgres  
        cursor.execute(create_raw_accommodation_bookings_tbl)
        cursor.execute(check_if_raw_accommodation_bookings_tbl_exists)
        sql_result = cursor.fetchone()[0]
        root_logger.info(sql_result)
        if sql_result:
            root_logger.debug(f"")
            root_logger.info(f"=============================================================================================================================================================================")
            root_logger.info(f"TABLE CREATION SUCCESS: Managed to create {table_name} table in {db_layer_name}.  ")
            root_logger.info(f"SQL Query for validation check:  {check_if_raw_accommodation_bookings_tbl_exists} ")
            root_logger.info(f"=============================================================================================================================================================================")
            root_logger.debug(f"")
        else:
            root_logger.debug(f"")
            root_logger.error(f"==========================================================================================================================================================================")
            root_logger.error(f"TABLE CREATION FAILURE: Unable to create {table_name}... ")
            root_logger.error(f"SQL Query for validation check:  {check_if_raw_accommodation_bookings_tbl_exists} ")
            root_logger.error(f"==========================================================================================================================================================================")
            root_logger.debug(f"")



        # Add data lineage to table 
        cursor.execute(add_data_lineage_to_raw_accommodation_bookings_tbl)
        cursor.execute(check_if_data_lineage_fields_are_added_to_tbl)
        sql_results = cursor.fetchall()
        # root_logger.info(sql_results)
        if len(sql_results) == 6:
            root_logger.debug(f"")
            root_logger.info(f"=============================================================================================================================================================================")
            root_logger.info(f"DATA LINEAGE FIELDS CREATION SUCCESS: Managed to create data lineage columns in {schema_name}.{table_name}.  ")
            root_logger.info(f"SQL Query for validation check:  {check_if_data_lineage_fields_are_added_to_tbl} ")
            root_logger.info(f"=============================================================================================================================================================================")
            root_logger.debug(f"")
        else:
            root_logger.debug(f"")
            root_logger.error(f"==========================================================================================================================================================================")
            root_logger.error(f"DATA LINEAGE FIELDS CREATION FAILURE: Unable to create create data lineage columns in {schema_name}.{table_name}.... ")
            root_logger.error(f"SQL Query for validation check:  {check_if_data_lineage_fields_are_added_to_tbl} ")
            root_logger.error(f"==========================================================================================================================================================================")
            root_logger.debug(f"")



        # Add insert rows to table 
        cursor.execute(insert_accommodation_bookings_data)
        cursor.execute(check_if_rows_are_inserted_into_raw_accommodation_bookings_tbl)

        





        # Commit the changes made above 
        root_logger.info("Now saving changes made by SQL statements in Postgres DB....")
        postgres_connection.commit()


    except Exception as e:
            root_logger.info(e)
        
    finally:
        
        # Close the cursor if it exists 
        if cursor is not None:
            cursor.close()
            root_logger.info("")
            root_logger.info("Closing cursor.")

        # Close the database connection to Postgres if it exists 
        if postgres_connection is not None:
            postgres_connection.close()
            root_logger.info("")
            root_logger.info("Closing postgres connection.")



load_data_to_raw_layer(postgres_connection)

