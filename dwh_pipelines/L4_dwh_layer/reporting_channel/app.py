import os 
import time 
import psycopg2
import configparser
from pathlib import Path
import logging, coloredlogs
import pandas as pd

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
file_handler        =   logging.FileHandler('logs/L4_dwh_layer/user_access_layer/' + current_filepath + '.log', mode='w')
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
    database                =   config['postgres_airflow_config']['DWH_DB']
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
    database                =   config['travel_data_filepath']['DWH_DB']
    username                =   config['travel_data_filepath']['USERNAME']
    password                =   config['travel_data_filepath']['PASSWORD']

    postgres_connection     =   None
    cursor                  =   None



# Begin the data extraction process
root_logger.info("")
root_logger.info("---------------------------------------------")
root_logger.info("Beginning the dwh process...")


postgres_connection = psycopg2.connect(
                host        =   host,
                port        =   port,
                dbname      =   database,
                user        =   username,
                password    =   password,
        )




def query_postgres_dwh(postgres_connection):
    try:
        
        # Set up constants
        
        active_schema_name              =   'reporting'
        active_db_name                  =    database
        table_name                      =   'top_destination'
        data_warehouse_layer            =   'DWH - UAL'
        column_index                    =   0 
        total_null_values_in_table      =   0 
        


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
        
    

        # ================================================== ENABLING CROSS-DATABASE QUERYING VIA FDW ==================================================

        # Set up SQL statements for schema creation and validation check 
        try:
             
            create_schema   =    f'''    CREATE SCHEMA IF NOT EXISTS {active_schema_name};
            '''

            check_if_schema_exists  =   f'''   SELECT schema_name from information_schema.schemata WHERE schema_name= '{active_schema_name}';
            '''


            # Create schema in Postgres
            CREATING_SCHEMA_PROCESSING_START_TIME   =   time.time()
            cursor.execute(create_schema)
            root_logger.info("")
            root_logger.info(f"Successfully created {active_schema_name} schema. ")
            root_logger.info("")
            CREATING_SCHEMA_PROCESSING_END_TIME     =   time.time()


            CREATING_SCHEMA_VAL_CHECK_START_TIME    =   time.time()
            cursor.execute(check_if_schema_exists)
            CREATING_SCHEMA_VAL_CHECK_END_TIME      =   time.time()

            

            sql_result = cursor.fetchone()[0]
            if sql_result:
                root_logger.debug(f"")
                root_logger.info(f"=================================================================================================")
                root_logger.info(f"SCHEMA CREATION SUCCESS: Managed to create '{active_schema_name}' schema in '{active_db_name}' ")
                root_logger.info(f"Schema name in Postgres: '{sql_result}' ")
                root_logger.info(f"SQL Query for validation check:  {check_if_schema_exists} ")
                root_logger.info(f"=================================================================================================")
                root_logger.debug(f"")

            else:
                root_logger.debug(f"")
                root_logger.error(f"=================================================================================================")
                root_logger.error(f"SCHEMA CREATION FAILURE: Unable to create schema for '{active_db_name}'...")
                root_logger.info(f"SQL Query for validation check:  {check_if_schema_exists} ")
                root_logger.error(f"=================================================================================================")
                root_logger.debug(f"")

            postgres_connection.commit()

        except psycopg2.Error as e:
            print(e)




        # ================================================== CREATE DASHBOARD VIA PLOTLY-DASH =======================================
        

        





        root_logger.info(f'')
        root_logger.info('================================================')



        # Commit the changes made in Postgres 
        root_logger.info("Now saving changes made by SQL statements to Postgres DB....")
        postgres_connection.commit()
        root_logger.info("Saved successfully, now terminating cursor and current session....")


    except psycopg2.Error as e:
            root_logger.info(e)
        
    finally:
        
        # Close the cursor if it exists 
        if cursor is not None:
            cursor.close()
            root_logger.debug("")
            root_logger.debug("Cursor closed successfully.")

        # Close the database connection to Postgres if it exists 
        if postgres_connection is not None:
            postgres_connection.close()
            root_logger.debug("Session connected to Postgres database closed.")



query_postgres_dwh(postgres_connection)

