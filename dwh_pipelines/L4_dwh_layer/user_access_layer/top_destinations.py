import os 
import time 
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




        # ================================================== CREATE AGGREGATE TABLE =======================================
        

        # Set up SQL statements for table deletion and validation check  
        delete_aggregate_tbl_if_exists     =   f''' DROP TABLE IF EXISTS {active_schema_name}.{table_name} CASCADE;
        '''

        check_if_aggregate_tbl_is_deleted  =   f'''   SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}' );
        '''

        # Set up SQL statements for table creation and validation check 
        create_aggregate_tbl = f'''                CREATE TABLE IF NOT EXISTS {active_schema_name}.{table_name}  AS
                                                                        SELECT 
                                                                            d.arrival_city as destination
                                                                            ,COUNT(*) as no_of_bookings
                                                                        FROM 
                                                                            live.dim_flights_tbl f
                                                                            INNER JOIN live.dim_destinations_tbl d 
                                                                                ON f.flight_id=d.flight_id
                                                                        GROUP BY 
                                                                            d.arrival_city
                                                                        ORDER BY 
                                                                            no_of_bookings DESC
                                                                        LIMIT 10

        '''
 
        check_if_aggregate_tbl_exists  =   f'''       SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}' );
        '''

       
        count_total_no_of_columns_in_table  =   f'''            SELECT          COUNT(column_name) 
                                                                FROM            information_schema.columns 
                                                                WHERE           table_name      =   '{table_name}'
                                                                AND             table_schema    =   '{active_schema_name}'
        '''

        count_total_no_of_unique_records_in_table   =   f'''        SELECT COUNT(*) FROM 
                                                                            (SELECT DISTINCT * FROM {active_schema_name}.{table_name}) as unique_records   
        '''

        get_list_of_column_names    =   f'''                SELECT column_name FROM information_schema.columns 
                                                            WHERE   table_name = '{table_name}'
                                                            ORDER BY ordinal_position 
        '''

        


        # Delete table if it exists in Postgres
        DELETING_SCHEMA_PROCESSING_START_TIME   =   time.time()
        cursor.execute(delete_aggregate_tbl_if_exists)
        DELETING_SCHEMA_PROCESSING_END_TIME     =   time.time()

        
        DELETING_SCHEMA_VAL_CHECK_PROCESSING_START_TIME     =   time.time()
        cursor.execute(check_if_aggregate_tbl_is_deleted)
        DELETING_SCHEMA_VAL_CHECK_PROCESSING_END_TIME       =   time.time()


        sql_result = cursor.fetchone()[0]
        if sql_result:
            root_logger.debug(f"")
            root_logger.info(f"=============================================================================================================================================================================")
            root_logger.info(f"TABLE DELETION SUCCESS: Managed to drop {table_name} table in {active_db_name}. Now advancing to recreating table... ")
            root_logger.info(f"SQL Query for validation check:  {check_if_aggregate_tbl_is_deleted} ")
            root_logger.info(f"=============================================================================================================================================================================")
            root_logger.debug(f"")
        else:
            root_logger.debug(f"")
            root_logger.error(f"==========================================================================================================================================================================")
            root_logger.error(f"TABLE DELETION FAILURE: Unable to delete {table_name}. This table may have objects that depend on it (use DROP TABLE ... CASCADE to resolve) or it doesn't exist. ")
            root_logger.error(f"SQL Query for validation check:  {check_if_aggregate_tbl_is_deleted} ")
            root_logger.error(f"==========================================================================================================================================================================")
            root_logger.debug(f"")



        # Create table if it doesn't exist in Postgres  
        CREATING_TABLE_PROCESSING_START_TIME    =   time.time()
        cursor.execute(create_aggregate_tbl)
        CREATING_TABLE_PROCESSING_END_TIME  =   time.time()

        
        CREATING_TABLE_VAL_CHECK_PROCESSING_START_TIME  =   time.time()
        cursor.execute(check_if_aggregate_tbl_exists)
        CREATING_TABLE_VAL_CHECK_PROCESSING_END_TIME    =   time.time()


        sql_result = cursor.fetchone()[0]
        if sql_result:
            root_logger.debug(f"")
            root_logger.info(f"=============================================================================================================================================================================")
            root_logger.info(f"TABLE CREATION SUCCESS: Managed to create {table_name} table in {active_db_name}.  ")
            root_logger.info(f"SQL Query for validation check:  {check_if_aggregate_tbl_exists} ")
            root_logger.info(f"=============================================================================================================================================================================")
            root_logger.debug(f"")
        else:
            root_logger.debug(f"")
            root_logger.error(f"==========================================================================================================================================================================")
            root_logger.error(f"TABLE CREATION FAILURE: Unable to create {table_name}... ")
            root_logger.error(f"SQL Query for validation check:  {check_if_aggregate_tbl_exists} ")
            root_logger.error(f"==========================================================================================================================================================================")
            root_logger.debug(f"")



    



        # ======================================= DATA PROFILING METRICS =======================================


        # Prepare data profiling metrics 


        # --------- A. Table statistics 
        cursor.execute(count_total_no_of_columns_in_table)
        total_columns_in_table = cursor.fetchone()[0]

        cursor.execute(count_total_no_of_unique_records_in_table)
        total_unique_records_in_table = cursor.fetchone()[0]
        

        cursor.execute(get_list_of_column_names)
        list_of_column_names = cursor.fetchall()
        column_names = [sql_result[0] for sql_result in list_of_column_names]
        
        
        # --------- B. Performance statistics (Python)
        EXECUTION_TIME_FOR_CREATING_SCHEMA                   =   (CREATING_SCHEMA_PROCESSING_END_TIME                -       CREATING_SCHEMA_PROCESSING_START_TIME                   )   * 1000


        EXECUTION_TIME_FOR_CREATING_SCHEMA_VAL_CHECK         =   (CREATING_SCHEMA_VAL_CHECK_END_TIME                 -       CREATING_SCHEMA_VAL_CHECK_START_TIME                    )   * 1000


        EXECUTION_TIME_FOR_DROPPING_SCHEMA                   =   (DELETING_SCHEMA_PROCESSING_END_TIME                -       DELETING_SCHEMA_PROCESSING_START_TIME                   )   * 1000


        EXECUTION_TIME_FOR_DROPPING_SCHEMA_VAL_CHECK         =   (DELETING_SCHEMA_VAL_CHECK_PROCESSING_END_TIME      -       DELETING_SCHEMA_VAL_CHECK_PROCESSING_START_TIME         )   * 1000


        EXECUTION_TIME_FOR_CREATING_TABLE                    =   (CREATING_TABLE_PROCESSING_END_TIME                 -       CREATING_TABLE_PROCESSING_START_TIME                    )   * 1000


        EXECUTION_TIME_FOR_CREATING_TABLE_VAL_CHECK          =   (CREATING_TABLE_VAL_CHECK_PROCESSING_END_TIME       -       CREATING_TABLE_VAL_CHECK_PROCESSING_START_TIME          )   * 1000




        # Display data profiling metrics
        
        root_logger.info(f'')
        root_logger.info(f'')
        root_logger.info('================================================')
        root_logger.info('              DATA PROFILING METRICS              ')
        root_logger.info('================================================')
        root_logger.info(f'')
        root_logger.info(f'Now calculating table statistics...')
        root_logger.info(f'')
        root_logger.info(f'')
        root_logger.info(f'Table name:                                  {table_name} ')
        root_logger.info(f'Schema name:                                 {active_schema_name} ')
        root_logger.info(f'Database name:                               {database} ')
        root_logger.info(f'Data warehouse layer:                        {data_warehouse_layer} ')
        root_logger.info(f'')
        root_logger.info(f'')
        root_logger.info(f'Number of columns in table:                  {total_columns_in_table} ')
        root_logger.info(f'')


        for column_name in column_names:
            cursor.execute(f'''
                    SELECT COUNT(*)
                    FROM {active_schema_name}.{table_name}
                    WHERE {column_name} is NULL
            ''')
            sql_result = cursor.fetchone()[0]
            total_null_values_in_table += sql_result
            column_index += 1
            if sql_result == 0:
                root_logger.info(f'Column name: {column_name},  Column no: {column_index},  Number of NULL values: {sql_result} ')
            else:
                root_logger.warning(f'Column name: {column_name},  Column no: {column_index},  Number of NULL values: {sql_result} ')
        



        root_logger.info(f'')
        root_logger.info('================================================')
        root_logger.info(f'')
        root_logger.info(f'Now calculating performance statistics (from a Python standpoint)...')
        root_logger.info(f'')
        root_logger.info(f'')


        if (EXECUTION_TIME_FOR_CREATING_SCHEMA > 1000) and (EXECUTION_TIME_FOR_CREATING_SCHEMA < 60000):
            root_logger.info(f'1. Execution time for CREATING schema: {EXECUTION_TIME_FOR_CREATING_SCHEMA} ms ({    round   (EXECUTION_TIME_FOR_CREATING_SCHEMA  /   1000, 2)   } secs) ')
            root_logger.info(f'')
            root_logger.info(f'')
        elif (EXECUTION_TIME_FOR_CREATING_SCHEMA >= 60000):
            root_logger.info(f'1. Execution time for CREATING schema: {EXECUTION_TIME_FOR_CREATING_SCHEMA} ms  ({    round   (EXECUTION_TIME_FOR_CREATING_SCHEMA  /   1000, 2)   } secs)  ({   round  ((EXECUTION_TIME_FOR_CREATING_SCHEMA  /   1000) / 60, 4)     } mins)   ')
            root_logger.info(f'')
            root_logger.info(f'')
        else:
            root_logger.info(f'1. Execution time for CREATING schema: {EXECUTION_TIME_FOR_CREATING_SCHEMA} ms ')
            root_logger.info(f'')
            root_logger.info(f'')



        if (EXECUTION_TIME_FOR_CREATING_SCHEMA_VAL_CHECK > 1000) and (EXECUTION_TIME_FOR_CREATING_SCHEMA_VAL_CHECK < 60000):
            root_logger.info(f'2. Execution time for CREATING schema (VAL CHECK): {EXECUTION_TIME_FOR_CREATING_SCHEMA_VAL_CHECK} ms ({  round   (EXECUTION_TIME_FOR_CREATING_SCHEMA_VAL_CHECK  /   1000, 2)} secs)      ')
            root_logger.info(f'')
            root_logger.info(f'')
        elif (EXECUTION_TIME_FOR_CREATING_SCHEMA_VAL_CHECK >= 60000):
            root_logger.info(f'2. Execution time for CREATING schema (VAL CHECK): {EXECUTION_TIME_FOR_CREATING_SCHEMA_VAL_CHECK} ms ({  round   (EXECUTION_TIME_FOR_CREATING_SCHEMA_VAL_CHECK  /   1000, 2)} secs)    ({  round ((EXECUTION_TIME_FOR_CREATING_SCHEMA_VAL_CHECK  /   1000) / 60,  4)   } min)      ')
            root_logger.info(f'')
            root_logger.info(f'')
        else:
            root_logger.info(f'2. Execution time for CREATING schema (VAL CHECK): {EXECUTION_TIME_FOR_CREATING_SCHEMA_VAL_CHECK} ms ')
            root_logger.info(f'')
            root_logger.info(f'')
        

        if (EXECUTION_TIME_FOR_DROPPING_SCHEMA > 1000) and (EXECUTION_TIME_FOR_DROPPING_SCHEMA < 60000):
            root_logger.info(f'3. Execution time for DELETING schema:  {EXECUTION_TIME_FOR_DROPPING_SCHEMA} ms ({  round   (EXECUTION_TIME_FOR_DROPPING_SCHEMA  /   1000, 2)} secs)      ')
            root_logger.info(f'')
            root_logger.info(f'')
        elif (EXECUTION_TIME_FOR_DROPPING_SCHEMA >= 60000):
            root_logger.info(f'3. Execution time for DELETING schema:  {EXECUTION_TIME_FOR_DROPPING_SCHEMA} ms ({  round   (EXECUTION_TIME_FOR_DROPPING_SCHEMA  /   1000, 2)} secs)    ({  round ((EXECUTION_TIME_FOR_DROPPING_SCHEMA  /   1000) / 60,  4)   } min)      ')
            root_logger.info(f'')
            root_logger.info(f'')
        else:
            root_logger.info(f'3. Execution time for DELETING schema:  {EXECUTION_TIME_FOR_DROPPING_SCHEMA} ms ')
            root_logger.info(f'')
            root_logger.info(f'')



        if (EXECUTION_TIME_FOR_DROPPING_SCHEMA_VAL_CHECK > 1000) and (EXECUTION_TIME_FOR_DROPPING_SCHEMA_VAL_CHECK < 60000):
            root_logger.info(f'4. Execution time for DELETING schema (VAL CHECK):  {EXECUTION_TIME_FOR_DROPPING_SCHEMA_VAL_CHECK} ms ({  round   (EXECUTION_TIME_FOR_DROPPING_SCHEMA_VAL_CHECK  /   1000, 2)} secs)      ')
            root_logger.info(f'')
            root_logger.info(f'')
        elif (EXECUTION_TIME_FOR_DROPPING_SCHEMA_VAL_CHECK >= 60000):
            root_logger.info(f'4. Execution time for DELETING schema (VAL CHECK):  {EXECUTION_TIME_FOR_DROPPING_SCHEMA_VAL_CHECK} ms ({  round   (EXECUTION_TIME_FOR_DROPPING_SCHEMA_VAL_CHECK  /   1000, 2)} secs)    ({  round ((EXECUTION_TIME_FOR_DROPPING_SCHEMA_VAL_CHECK  /   1000) / 60,  4)   } min)      ')
            root_logger.info(f'')
            root_logger.info(f'')
        else:
            root_logger.info(f'4. Execution time for DELETING schema (VAL CHECK):  {EXECUTION_TIME_FOR_DROPPING_SCHEMA_VAL_CHECK} ms ')
            root_logger.info(f'')
            root_logger.info(f'')

        

        if (EXECUTION_TIME_FOR_CREATING_TABLE > 1000) and (EXECUTION_TIME_FOR_CREATING_TABLE < 60000):
            root_logger.info(f'5. Execution time for CREATING table:  {EXECUTION_TIME_FOR_CREATING_TABLE} ms ({  round   (EXECUTION_TIME_FOR_CREATING_TABLE  /   1000, 2)} secs)      ')
            root_logger.info(f'')
            root_logger.info(f'')
        elif (EXECUTION_TIME_FOR_CREATING_TABLE >= 60000):
            root_logger.info(f'5. Execution time for CREATING table:  {EXECUTION_TIME_FOR_CREATING_TABLE} ms ({  round   (EXECUTION_TIME_FOR_CREATING_TABLE  /   1000, 2)} secs)    ({  round ((EXECUTION_TIME_FOR_CREATING_TABLE  /   1000) / 60,  4)   } min)      ')
            root_logger.info(f'')
            root_logger.info(f'')
        else:
            root_logger.info(f'5. Execution time for CREATING table:  {EXECUTION_TIME_FOR_CREATING_TABLE} ms ')
            root_logger.info(f'')
            root_logger.info(f'')



        if (EXECUTION_TIME_FOR_CREATING_TABLE_VAL_CHECK > 1000) and (EXECUTION_TIME_FOR_CREATING_TABLE_VAL_CHECK < 60000):
            root_logger.info(f'6. Execution time for CREATING table (VAL CHECK):  {EXECUTION_TIME_FOR_CREATING_TABLE_VAL_CHECK} ms ({  round   (EXECUTION_TIME_FOR_CREATING_TABLE_VAL_CHECK  /   1000, 2)} secs)      ')
            root_logger.info(f'')
            root_logger.info(f'')
        elif (EXECUTION_TIME_FOR_CREATING_TABLE_VAL_CHECK >= 60000):
            root_logger.info(f'6. Execution time for CREATING table (VAL CHECK):  {EXECUTION_TIME_FOR_CREATING_TABLE_VAL_CHECK} ms ({  round   (EXECUTION_TIME_FOR_CREATING_TABLE_VAL_CHECK  /   1000, 2)} secs)  ({  round ((EXECUTION_TIME_FOR_CREATING_TABLE_VAL_CHECK  /   1000) / 60,  4)   } min)      ')
            root_logger.info(f'')
            root_logger.info(f'')
        else:
            root_logger.info(f'6. Execution time for CREATING table (VAL CHECK):  {EXECUTION_TIME_FOR_CREATING_TABLE_VAL_CHECK} ms ')
            root_logger.info(f'')
            root_logger.info(f'')



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

