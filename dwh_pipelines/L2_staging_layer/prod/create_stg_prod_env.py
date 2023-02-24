import os 
import json
import time 
import random
import psycopg2
import pandas as pd
import configparser
from pathlib import Path
import logging, coloredlogs
from datetime import datetime

# ================================================ LOGGER ================================================


# Set up root root_logger 
root_logger     =   logging.getLogger(__name__)
root_logger.setLevel(logging.DEBUG)


# Set up formatter for logs 
file_handler_log_formatter      =   logging.Formatter('%(asctime)s  |  %(levelname)s  |  %(message)s  ')
console_handler_log_formatter   =   coloredlogs.ColoredFormatter(fmt    =   '%(message)s', level_styles=dict(
                                                                                                debug           =   dict    (color  =   'white'),
                                                                                                info            =   dict    (color  =   'green'),
                                                                                                warning         =   dict    (color  =   'cyan'),
                                                                                                error           =   dict    (color  =   'red',      bold    =   True,   bright      =   True),
                                                                                                critical        =   dict    (color  =   'black',    bold    =   True,   background  =   'red')
                                                                                            ),

                                                                                    field_styles=dict(
                                                                                        messages            =   dict    (color  =   'white')
                                                                                    )
                                                                                    )


# Set up file handler object for logging events to file
current_filepath    =   Path(__file__).stem
file_handler        =   logging.FileHandler('logs/L2_staging_layer/prod/' + current_filepath + '.log', mode='w')
file_handler.setFormatter(file_handler_log_formatter)


# Set up console handler object for writing event logs to console in real time (i.e. streams events to stderr)
console_handler     =   logging.StreamHandler()
console_handler.setFormatter(console_handler_log_formatter)


# Add the file handler 
root_logger.addHandler(file_handler)


# Only add the console handler if the script is running directly from this location 
if __name__=="__main__":
    root_logger.addHandler(console_handler)




# ================================================ CONFIG ================================================
config  =   configparser.ConfigParser()

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



# Connect to the staging instance of the Postgres data warehouse

postgres_connection = psycopg2.connect(
                host        =   host,
                port        =   port,
                dbname      =   database,
                user        =   username,
                password    =   password,
        )
postgres_connection.set_session(autocommit=True)



def create_prod_environment_for_staging():
        # Set up constants
        CURRENT_TIMESTAMP               =   datetime.now()
        dev_schema_name                 =   'dev'
        prod_schema_name                =   'prod'
        active_db_name                  =    database
        data_warehouse_layer            =   'STAGING'
        
    
        # Create a cursor object to execute the PG-SQL commands 
        cursor      =   postgres_connection.cursor()



        # Validate the Postgres database connection
        if postgres_connection.closed == 0:
            root_logger.debug(f"")
            root_logger.info("=================================================================================")
            root_logger.info(f"CONNECTION SUCCESS: Managed to connect successfully to the {active_db_name} database!!")
            root_logger.info(f"Connection details: {postgres_connection.dsn} ")
            root_logger.info("=================================================================================")
            root_logger.debug("")
        
        elif postgres_connection.closed != 0:
            raise ConnectionError("CONNECTION ERROR: Unable to connect to the demo_company database...") 
        


        # Set up SQL statements for schema creation and validation check 
        try:
             
            create_schema   =    f'''    CREATE SCHEMA IF NOT EXISTS {prod_schema_name};
            '''

            check_if_schema_exists  =   f'''   SELECT schema_name from information_schema.schemata WHERE schema_name= '{prod_schema_name}';
            '''


            # Create schema in Postgres
            CREATING_SCHEMA_PROCESSING_START_TIME   =   time.time()
            cursor.execute(create_schema)
            root_logger.info("")
            root_logger.info(f"Successfully created '{prod_schema_name}' schema. ")
            root_logger.info("")
            CREATING_SCHEMA_PROCESSING_END_TIME     =   time.time()


            CREATING_SCHEMA_VAL_CHECK_START_TIME    =   time.time()
            cursor.execute(check_if_schema_exists)
            CREATING_SCHEMA_VAL_CHECK_END_TIME      =   time.time()

            

            sql_result = cursor.fetchone()[0]
            if sql_result:
                root_logger.debug(f"")
                root_logger.info(f"=================================================================================================")
                root_logger.info(f"SCHEMA CREATION SUCCESS: Managed to create {prod_schema_name} schema in {active_db_name} ")
                root_logger.info(f"Schema name in Postgres: {sql_result} ")
                root_logger.info(f"SQL Query for validation check:  {check_if_schema_exists} ")
                root_logger.info(f"=================================================================================================")
                root_logger.debug(f"")

            else:
                root_logger.debug(f"")
                root_logger.error(f"=================================================================================================")
                root_logger.error(f"SCHEMA CREATION FAILURE: Unable to create schema for {active_db_name}...")
                root_logger.info(f"SQL Query for validation check:  {check_if_schema_exists} ")
                root_logger.error(f"=================================================================================================")
                root_logger.debug(f"")

            # postgres_connection.commit()

        except Exception as e:
            print(e)


        # Get all the tables from DEV environment 
        try:
            root_logger.debug(f"")
            root_logger.debug(f"Now creating '{prod_schema_name}' environment ....")
            root_logger.debug(f"")
            sql_query = f"""    SELECT table_name FROM information_schema.tables WHERE table_schema = '{dev_schema_name}' AND    table_name LIKE '%stg%' 
            """
            cursor.execute(sql_query)

            sql_results = cursor.fetchall()
            no_of_sql_results = len(sql_results)
            root_logger.debug(f'No of results: {no_of_sql_results} ')


            for table in sql_results:
                table_name = table[0]
                root_logger.info(f"")
                root_logger.info(f"Now creating '{table_name}' table in production environment ...")
                # root_logger.info(f"")
                sql_query = f"""  CREATE TABLE IF NOT EXISTS {prod_schema_name}.{table_name} as SELECT * FROM {dev_schema_name}.{table_name}
                """
                cursor.execute(sql_query)
                # root_logger.info(f"")
                root_logger.info(f"Successfully created '{table_name}' table in production environment ")
                root_logger.info(f"")
            
        
            # postgres_connection.commit()
            root_logger.debug(f"")
            root_logger.debug(f"Successfully created '{prod_schema_name}' environment. ")
            root_logger.debug(f"")
                  


        except Exception as e:
             print(e)

        
        finally:
            
            # Close the cursor if it exists 
            if cursor is not None:
                cursor.close()
                root_logger.debug("")
                root_logger.debug("Cursor closed successfully.")

            # Close the database connection to Postgres if it exists 
            if postgres_connection is not None:
                postgres_connection.close()
                # root_logger.debug("")
                root_logger.debug("Session connected to Postgres database closed.")



create_prod_environment_for_staging()