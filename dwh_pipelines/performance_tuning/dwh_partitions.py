import os 
import psycopg2
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
postgres_connection.set_session(autocommit=True)


def set_up_partitions(postgres_connection):
    try:
        
        # Set up constants
        
        cursor                                                  =          postgres_connection.cursor()
        active_db_name                                          =          database

        dwh_reporting_schema                                    =           'reporting'
        dwh_live_schema                                         =           'live'


        # For creating partitions
        src_table = 'total_sales_by_payment_method'
        parent_table = 'total_sales_by_payment_method_parent_tbl'
        child_table_1 = 'total_sales_by_payment_method_bank_transfer'
        child_table_2 = 'total_sales_by_payment_method_credit_card'
        child_table_3 = 'total_sales_by_payment_method_debit_card'
        child_table_4 = 'total_sales_by_payment_method_paypal'

        partition_value_category_1 = 'Bank transfer'
        partition_value_category_2 = 'Credit card'
        partition_value_category_3 = 'Debit card'
        partition_value_category_4 = 'PayPal'
        child_tables = [child_table_1, child_table_2, child_table_3, child_table_4]

        

        create_parent_table = f'''   CREATE TABLE {dwh_reporting_schema}.{parent_table} (
                                                        total_revenue           INTEGER, 
                                                        payment_method          VARCHAR NOT NULL, 
                                                        booking_year            INTEGER

                                                    ) 
                                                    PARTITION BY LIST   (payment_method);
        '''

        create_partition_table_1 = f'''     CREATE TABLE {child_table_1}
                                                    PARTITION OF {dwh_reporting_schema}.{parent_table}
                                                    FOR VALUES IN ('{partition_value_category_1}')
                                                    ;
        ;
        '''

        create_partition_table_2 = f'''    CREATE TABLE {child_table_2}
                                                    PARTITION OF {dwh_reporting_schema}.{parent_table}
                                                    FOR VALUES IN ('{partition_value_category_2}')
                                                    ;
        ;
        '''

        create_partition_table_3 = f'''    CREATE TABLE {child_table_3}
                                                    PARTITION OF {dwh_reporting_schema}.{parent_table}
                                                    FOR VALUES IN ('{partition_value_category_3}')
                                                    ;
        ;
        '''

        create_partition_table_4 = f'''    CREATE TABLE {child_table_4}
                                                    PARTITION OF {dwh_reporting_schema}.{parent_table}
                                                    FOR VALUES IN ('{partition_value_category_4}')
                                                    ;
        ;
        '''

        insert_data_into_partition_table_1 = f'''    INSERT INTO {child_table_1}
                                                        SELECT
                                                            total_revenue, 
                                                            payment_method, 
                                                            booking_year
                                                        FROM 
                                                            reporting.total_sales_by_payment_method
                                                        WHERE 
                                                            payment_method = '{partition_value_category_1}'
                                                            ;       
        '''

        insert_data_into_partition_table_2 = f'''    INSERT INTO {child_table_2}
                                                        SELECT
                                                            total_revenue, 
                                                            payment_method, 
                                                            booking_year
                                                        FROM 
                                                            reporting.total_sales_by_payment_method
                                                        WHERE 
                                                            payment_method = '{partition_value_category_2}'
                                                            ;      
        '''

        insert_data_into_partition_table_3 = f'''    INSERT INTO {child_table_3}
                                                        SELECT
                                                            total_revenue, 
                                                            payment_method, 
                                                            booking_year
                                                        FROM 
                                                            reporting.total_sales_by_payment_method
                                                        WHERE 
                                                            payment_method = '{partition_value_category_3}'
                                                            ;    
        '''

        insert_data_into_partition_table_4 = f'''    INSERT INTO {child_table_4}
                                                        SELECT
                                                            total_revenue, 
                                                            payment_method, 
                                                            booking_year
                                                        FROM 
                                                            reporting.total_sales_by_payment_method
                                                        WHERE 
                                                            payment_method = '{partition_value_category_4}'
                                                            ;       
        '''


        check_if_parent_table_exists_already = f''' SELECT relname FROM pg_class WHERE relname = '{parent_table}' ;
        '''


        drop_partition_table_1 = f'''   DROP TABLE IF EXISTS {child_table_1} ; 
        '''
        
        drop_partition_table_2 = f'''   DROP TABLE IF EXISTS {child_table_2} ; 
        '''
        
        drop_partition_table_3 = f'''   DROP TABLE IF EXISTS {child_table_3} ; 
        '''
        
        drop_partition_table_4 = f'''   DROP TABLE IF EXISTS {child_table_4} ; 
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
        




        # ================================================== CREATE PARTITIONS =======================================

        try:
            root_logger.info(f'=========================================== CREATE PARTITIONS =======================================')
            root_logger.info(f'======================================================================================================')
            root_logger.info(f'')
            root_logger.info(f'')

        
       
            try:
                cursor.execute(create_partition_table_1)
                root_logger.info(f'''Successfully created '{child_table_1}'  partition table ...  ''')
                root_logger.info(f'-------------------------------------------------------------')
                root_logger.info(f'')
                root_logger.info(f'')
                root_logger.info(f'')
                root_logger.info(f'')

            except psycopg2.Error as e: 
                root_logger.warning(f"The '{child_table_1}' partition table already exists....now dropping and recreating ...")
                root_logger.info("")
                cursor.execute(drop_partition_table_1)
                cursor.execute(create_partition_table_1)
                root_logger.info(f'''Successfully created '{child_table_1}'  partition table ...  ''')
                root_logger.info(f'-------------------------------------------------------------')
                root_logger.info(f'')
                root_logger.info(f'')
                root_logger.info(f'')
                root_logger.info(f'')
        

            try:
                cursor.execute(create_partition_table_2)
                root_logger.info(f'''Successfully created '{child_table_2}'  partition table ...  ''')
                root_logger.info(f'-------------------------------------------------------------')
                root_logger.info(f'')
                root_logger.info(f'')
                root_logger.info(f'')
                root_logger.info(f'')

            except psycopg2.Error as e: 
                root_logger.warning(f"The '{child_table_2}' partition table already exists....now dropping and recreating ...")
                root_logger.info("")
                cursor.execute(drop_partition_table_2)
                cursor.execute(create_partition_table_2)
                root_logger.info(f'''Successfully created '{child_table_2}'  partition table ...  ''')
                root_logger.info(f'-------------------------------------------------------------')
                root_logger.info(f'')
                root_logger.info(f'')
                root_logger.info(f'')
                root_logger.info(f'')

        

            try:
                cursor.execute(create_partition_table_3)
                root_logger.info(f'''Successfully created '{child_table_3}'  partition table ...  ''')
                root_logger.info(f'-------------------------------------------------------------')
                root_logger.info(f'')
                root_logger.info(f'')
                root_logger.info(f'')
                root_logger.info(f'')

            except psycopg2.Error as e: 
                root_logger.warning(f"The '{child_table_3}' partition table already exists....now dropping and recreating ...")
                root_logger.info("")
                cursor.execute(drop_partition_table_3)
                cursor.execute(create_partition_table_3)
                root_logger.info(f'''Successfully created '{child_table_3}'  partition table ...  ''')
                root_logger.info(f'-------------------------------------------------------------')
                root_logger.info(f'')
                root_logger.info(f'')
                root_logger.info(f'')
                root_logger.info(f'')



            try:
                cursor.execute(create_partition_table_4)
                root_logger.info(f'''Successfully created '{child_table_4}'  partition table ...  ''')
                root_logger.info(f'-------------------------------------------------------------')
                root_logger.info(f'')
                root_logger.info(f'')
                root_logger.info(f'')
                root_logger.info(f'')

            except psycopg2.Error as e: 
                root_logger.warning(f"The '{child_table_4}' partition table already exists....now dropping and recreating ...")
                root_logger.info("")
                cursor.execute(drop_partition_table_4)
                cursor.execute(create_partition_table_4)
                root_logger.info(f'''Successfully created '{child_table_4}'  partition table ...  ''')
                root_logger.info(f'-------------------------------------------------------------')
                root_logger.info(f'')
                root_logger.info(f'')
                root_logger.info(f'')
                root_logger.info(f'')



            root_logger.info(f'-------------------------------------------------------------')
            root_logger.info(f'-------------------------------------------------------------')
            root_logger.info(f'Now inserting data into partition tables...')
            root_logger.info(f'-------------------------------------------------------------')
            root_logger.info(f'-------------------------------------------------------------')
            root_logger.info(f'')
            root_logger.info(f'')
            root_logger.info(f'')
            root_logger.info(f'')


            cursor.execute(insert_data_into_partition_table_1)
            # postgres_connection.commit()
            root_logger.info(f'''Successfully inserted data into '{child_table_1}'  partition table ...  ''')
            root_logger.info(f'-------------------------------------------------------------')
            root_logger.info(f'')
            root_logger.info(f'')
            root_logger.info(f'')
            root_logger.info(f'')

            cursor.execute(insert_data_into_partition_table_2)
            # postgres_connection.commit()
            root_logger.info(f'''Successfully inserted data into '{child_table_2}'  partition table ...  ''')
            root_logger.info(f'-------------------------------------------------------------')
            root_logger.info(f'')
            root_logger.info(f'')
            root_logger.info(f'')
            root_logger.info(f'')

            cursor.execute(insert_data_into_partition_table_3)
            # postgres_connection.commit()
            root_logger.info(f'''Successfully inserted data into '{child_table_3}'  partition table ...  ''')
            root_logger.info(f'-------------------------------------------------------------')
            root_logger.info(f'')
            root_logger.info(f'')
            root_logger.info(f'')
            root_logger.info(f'')


            cursor.execute(insert_data_into_partition_table_4)
            # postgres_connection.commit()
            root_logger.info(f'''Successfully inserted data into '{child_table_4}'  partition table ...  ''')
            root_logger.info(f'-------------------------------------------------------------')
            root_logger.info(f'')
            root_logger.info(f'')
            root_logger.info(f'')
            root_logger.info(f'')

        except psycopg2.Error as e:
            root_logger.error(e)


    except psycopg2.Error as e:
            root_logger.error(e)
        

set_up_partitions(postgres_connection)




# Miscellaneous scripts

'''

-- Check if indexes exist
SELECT * FROM pg_indexes WHERE schemaname != 'pg_catalog'

'''