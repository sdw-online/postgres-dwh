import psycopg2
import json
import configparser
import logging
from colorit import init_colorit, Colors, color
import pandas as pd 
from datetime import datetime
import os 
from pathlib import Path


# Set up root root_logger 
root_logger = logging.getLogger(__name__)
root_logger.setLevel(logging.DEBUG)



# Set up formatter for logs 
file_handler_log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s  ')
console_handler_log_formatter = logging.Formatter('%(message)s ')


# Set up file handler object for logging events to file
current_filepath = Path(__file__).stem
file_handler = logging.FileHandler(current_filepath + '.log', mode='w')
file_handler.setFormatter(file_handler_log_formatter)


# Set up console handler object for writing event logs to console in real time (i.e. streams events to stderr)
console_handler = logging.StreamHandler()
console_handler.setFormatter(console_handler_log_formatter)


# Add the file and console handlers 
root_logger.addHandler(file_handler)
root_logger.addHandler(console_handler)


# Add a flag/switch indicating whether Airflow is in use or not 
USING_AIRFLOW = False



# Create a config file for storing environment variables
config = configparser.ConfigParser()
if USING_AIRFLOW:

    # Use the airflow config file from the airflow container 
    config.read('/usr/local/airflow/dags/etl_to_postgres/airflow_config.ini')
    accommodation_bookings_path = config['postgres_airflow_config']['DATASET_SOURCE_PATH'] + "accommodation_bookings.json"

    host                = config['postgres_airflow_config']['HOST']
    port                = config['postgres_airflow_config']['PORT']
    database            = config['postgres_airflow_config']['DATABASE']
    username            = config['postgres_airflow_config']['USERNAME']
    password            = config['postgres_airflow_config']['PASSWORD']
    
    postgres_connection    = None
    cursor              = None

    
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

    postgres_connection    = None
    cursor              = None





with open(accommodation_bookings_path, 'r') as accommodation_bookings_file:        
    accommodation_bookings_data = json.load(accommodation_bookings_file)
    # accommodation_bookings_data = accommodation_bookings_data[0:100]
    




def _connect_to_postgres_database():
    try:
        postgres_connection = psycopg2.connect(
            host = host,
            port = port,
            dbname = database,
            user = username,
            password = password,
        )


        # Create a cursor object to execute the PG-SQL commands 
        cursor = postgres_connection.cursor()


        



        # Validate the Postgres database connection
        if postgres_connection.closed == 0:
            root_logger.info("=================================================================================")
            root_logger.info("CONNECTION SUCCESS: Managed to connect successfully to the demo_company database!!")
            root_logger.info("=================================================================================")
            root_logger.info("")
        
        elif postgres_connection.closed != 0:
            raise ConnectionError("CONNECTION ERROR: Unable to connect to the demo_company database...") 
        




        # ======================================= LOAD SRC TO RAW =======================================
        
        database = database

        create_raw_layer = f'''         CREATE DATABASE IF NOT EXISTS {database};
        
        ''' 

        cursor.execute(create_raw_layer)
        









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



_connect_to_postgres_database()

