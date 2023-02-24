import os 
import psycopg2
from psycopg2 import errors
import configparser
from pathlib import Path
import logging, coloredlogs


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
file_handler        =   logging.FileHandler('logs/governance/' + current_filepath + '.log', mode='w')
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



def set_up_access_controls(postgres_connection):
    try:
        
        # Set up constants
        
        cursor                                                  =          postgres_connection.cursor()
        active_db_name                                          =          database
        raw_db                                                  =          config['travel_data_filepath']['RAW_DB']
        staging_db                                              =          config['travel_data_filepath']['STAGING_DB']
        semantic_db                                             =          config['travel_data_filepath']['SEMANTIC_DB']
        dwh_db                                                  =          config['travel_data_filepath']['DWH_DB']
        custom_roles                                            =          ['junior_data_analyst',
                                                                          'senior_data_analyst',  
                                                                          'junior_data_engineer',   
                                                                          'senior_data_engineer', 
                                                                          'junior_data_scientist',
                                                                          'senior_data_scientist'
                                                                          ]
        
        raw_main_schema                                         =           'main'
        
        staging_dev_schema                                      =           'dev'
        staging_prod_schema                                     =           'prod'

        semantic_dev_schema                                     =           'dev'
        semantic_prod_schema                                    =           'prod'

        dwh_reporting_schema                                    =           'reporting'
        dwh_live_schema                                         =           'live'


        # For creating indexes
        table_1 = 'dim_destinations_tbl'
        table_2 = 'dim_flights_tbl'

        index_1 = 'idx_arrival_city'
        index_2 = 'idx_departure_city'
        index_3 = 'idx_ticket_price'

        column_1 = 'arrival_city'
        column_2 = 'departure_city'
        column_3 = 'ticket_price'


        create_index_sql_query_1 = f'''     CREATE INDEX {index_1} ON {dwh_live_schema}.{table_1}({column_1});
        '''
        create_index_sql_query_2 = f'''     CREATE INDEX {index_2} ON {dwh_live_schema}.{table_1}({column_2});
        '''
        create_index_sql_query_3 = f'''     CREATE INDEX {index_3} ON {dwh_live_schema}.{table_2}({column_3});
        '''



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
        




        # ================================================== CREATE INDEXES =======================================

        try:
            root_logger.info(f'=========================================== CREATE INDEXES =======================================')
            root_logger.info(f'======================================================================================================')
            root_logger.info(f'')
            root_logger.info(f'')

            cursor.execute(create_index_sql_query_1)
            postgres_connection.commit()
            root_logger.info(f'''Successfully created index '{index_1}'  index for table '{table_1}' table on '{column_1}' column   ''')
            root_logger.info(f'-------------------------------------------------------------')
            root_logger.info(f'')
            root_logger.info(f'')
            root_logger.info(f'')
            root_logger.info(f'')
        

            cursor.execute(create_index_sql_query_2)
            postgres_connection.commit()
            root_logger.info(f'''Successfully created index '{index_2}'  index for table '{table_1}' table on '{column_2}' column   ''')
            root_logger.info(f'-------------------------------------------------------------')
            root_logger.info(f'')
            root_logger.info(f'')
            root_logger.info(f'')
            root_logger.info(f'')
        

            cursor.execute(create_index_sql_query_3)
            postgres_connection.commit()
            root_logger.info(f'''Successfully created index '{index_3}'  index for table '{table_2}' table on '{column_3}' column   ''')
            root_logger.info(f'-------------------------------------------------------------')
            root_logger.info(f'')
            root_logger.info(f'')
            root_logger.info(f'')
            root_logger.info(f'')
        except psycopg2.Error as e:
            root_logger.error(e)


    except psycopg2.Error as e:
            root_logger.error(e)
        

set_up_access_controls(postgres_connection)





# Miscellaneous scripts

'''

-- Check if indexes exist
SELECT * FROM pg_indexes WHERE schemaname != 'pg_catalog'



'''