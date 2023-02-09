import psycopg2
import json
import configparser
import logging
from colorit import init_colorit, Colors, color
import pandas as pd 
from datetime import datetime
import os 
from pathlib import Path
import time 


# Set up root root_logger 
root_logger = logging.getLogger(__name__)
root_logger.setLevel(logging.DEBUG)



# Set up formatter for logs 
file_handler_log_formatter = logging.Formatter('%(asctime)s  |  %(levelname)s  |  %(message)s  ')
console_handler_log_formatter = logging.Formatter('%(message)s ')


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
        
        # Create a cursor object to execute the PG-SQL commands 
        cursor = postgres_connection.cursor()


        # Validate the Postgres database connection
        if postgres_connection.closed == 0:
            root_logger.debug(f"")
            root_logger.info("=================================================================================")
            root_logger.info("CONNECTION SUCCESS: Managed to connect successfully to the demo_company database!!")
            root_logger.info("=================================================================================")
            root_logger.debug("")
        
        elif postgres_connection.closed != 0:
            raise ConnectionError("CONNECTION ERROR: Unable to connect to the demo_company database...") 
        




        # ======================================= LOAD SRC TO RAW =======================================
        
        row_counter = 0 
        successful_rows_upload_count  =   0 
        failed_rows_upload_count      =   0 

        db_layer_name = database
        schema_name = 'main'
        table_name = 'accommodation_bookings_tbl'




        
        create_schema = f'''    CREATE SCHEMA IF NOT EXISTS {schema_name}
        '''

        check_if_schema_exists = f'''   SELECT schema_name from information_schema.schemata WHERE schema_name= '{schema_name}'
        '''
       
        delete_accommodation_bookings_tbl_if_exists = f''' DROP TABLE IF EXISTS {schema_name}.{table_name} CASCADE
        '''
        
        create_accommodation_bookings_tbl = f'''                CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
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


        add_data_lineage_to_accommodation_bookings_tbl = f'''        ALTER TABLE {schema_name}.{table_name}
                                                                        ADD COLUMN created_at           TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                                                                        ADD COLUMN updated_at           TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                                                                        ADD COLUMN source_system        VARCHAR(255),
                                                                        ADD COLUMN source_file          VARCHAR(255),
                                                                        ADD load_timestamp              TIMESTAMP,
                                                                        ADD transformation_process      VARCHAR(255)

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

                                                                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s);
        '''


        # Create schema for database
        cursor.execute(create_schema)
        cursor.execute(check_if_schema_exists)
        sql_result = cursor.fetchone()
        if len(sql_result) > 0:
            root_logger.debug(f"")
            root_logger.info(f"==============================================")
            root_logger.info(f"SCHEMA CREATION SUCCESS: Managed to create {schema_name} schema in {db_layer_name} ")
            root_logger.info(f"==============================================")
            root_logger.debug(f"")

        else:
            root_logger.debug(f"")
            root_logger.error(f"==============================================")
            root_logger.error(f"SCHEMA CREATION FAILURE: Unable to create schema for {db_layer_name}...")
            root_logger.error(f"==============================================")
            root_logger.debug(f"")

        
        # try:
        #     cursor.execute(create_schema)
        #     root_logger.debug(f"")
        #     root_logger.info(f"==============================================")
        #     root_logger.info(f"SCHEMA CREATION SUCCESS: Managed to create {schema_name} schema in {db_layer_name} ")
        #     root_logger.info(f"==============================================")
        #     root_logger.debug(f"")
        # except:
        #     root_logger.debug(f"")
        #     root_logger.error(f"==============================================")
        #     root_logger.error(f"SCHEMA CREATION FAILURE: Unable to create schema for {db_layer_name}...")
        #     root_logger.error(f"==============================================")
        #     root_logger.debug(f"")

        
        try:
            cursor(delete_accommodation_bookings_tbl_if_exists)
            root_logger.debug(f"")
            root_logger.info(f"==============================================")
            root_logger.info(f"TABLE DELETION SUCCESS: Managed to drop {table_name} table in {db_layer_name}. Now advancing to recreating table... ")
            root_logger.info(f"==============================================")
            root_logger.debug(f"")
        except:
            root_logger.debug(f"")
            root_logger.error(f"==============================================")
            root_logger.error(f"TABLE DELETION FAILURE: Unable to delete {table_name}. This table may have objects that depend on it (use DROP TABLE ... CASCADE to resolve) or it doesn't exist. ")
            root_logger.error(f"==============================================")
            root_logger.debug(f"")












    except Exception as e:
            root_logger.info(e)
        
    finally:
        
        # Close the cursor if it exists 
        if cursor is not None:
            root_logger.info(cursor)
            cursor.close()

        # Close the database connection to Postgres if it exists 
        if postgres_connection is not None:
            root_logger.info(postgres_connection)
            postgres_connection.close()



load_data_to_raw_layer(postgres_connection)

