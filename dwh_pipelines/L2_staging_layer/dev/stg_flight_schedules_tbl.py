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
file_handler        =   logging.FileHandler('logs/L2_staging_layer/dev/' + current_filepath + '.log', mode='w')
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



# Begin the data extraction process
root_logger.info("")
root_logger.info("---------------------------------------------")
root_logger.info("Beginning the staging process...")


postgres_connection = psycopg2.connect(
                host        =   host,
                port        =   port,
                dbname      =   database,
                user        =   username,
                password    =   password,
        )




def load_data_to_stg_flight_schedules_table(postgres_connection):
    try:
        
        # Set up constants
        CURRENT_TIMESTAMP               =   datetime.now()
        fdw_extension                   =   'postgres_fdw'
        foreign_server                  =   'raw_db_server'
        fdw_user                        =   username
        # fdw_user                        =   'fdw_user'
        previous_db_name                =   'raw_db'
        previous_schema_name            =   'main'
        active_schema_name              =   'dev'
        active_db_name                  =    database
        src_table_name                  =   'raw_flight_schedules_tbl'
        table_name                      =   'stg_flight_schedules_tbl'
        data_warehouse_layer            =   'STAGING'
        source_system                   =   ['CRM', 'ERP', 'Mobile App', 'Website', '3rd party apps', 'Company database']
        row_counter                     =   0 
        column_index                    =   0 
        total_null_values_in_table      =   0 
        successful_rows_upload_count    =   0 
        failed_rows_upload_count        =   0 
        


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
                root_logger.info(f"SCHEMA CREATION SUCCESS: Managed to create {active_schema_name} schema in {active_db_name} ")
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

            postgres_connection.commit()

        except Exception as e:
            print(e)



        # Drop extension postgres_fdw if it exists 
        try:
            drop_postgres_fdw_extension = f'''  DROP EXTENSION {fdw_extension} CASCADE
                                                ;   
            '''
            cursor.execute(drop_postgres_fdw_extension)
            postgres_connection.commit()


            root_logger.info("")
            root_logger.info(f"Successfully DROPPED the '{fdw_extension}' extension. Now advancing to re-importing the extension...")
            root_logger.info("")

            
        except Exception as e:
            print(e)

        

        # Create the postgres_fdw extension  
        try:
            import_postgres_fdw = f'''    CREATE EXTENSION {fdw_extension}
                                                ;   
            '''
            
            cursor.execute(import_postgres_fdw)
            postgres_connection.commit()


            root_logger.info("")
            root_logger.info(f"Successfully IMPORTED the '{fdw_extension}' extension. Now advancing to creating the foreign server...")
            root_logger.info("")
        except Exception as e:
            print(e)



        # Create the foreign server
        try: 
            create_foreign_server = f'''    CREATE SERVER {foreign_server}
                                                FOREIGN DATA WRAPPER {fdw_extension}
                                                OPTIONS (host '{host}', dbname '{previous_db_name}', port '{port}')
                                                ;
            '''
            cursor.execute(create_foreign_server)
            postgres_connection.commit()


            root_logger.info("")
            root_logger.info(f"Successfully CREATED the '{foreign_server}' foreign server. Now advancing to user mapping stage...")
            root_logger.info("")
        except Exception as e:
            print(e)


        
        # Create the user mapping between the fdw_user and local user 
        try:
            map_fdw_user_to_local_user = f'''       CREATE USER MAPPING FOR {username}
                                                        SERVER {foreign_server}
                                                        OPTIONS (user '{fdw_user}', password '{password}')
                                                        ;
            '''

            cursor.execute(map_fdw_user_to_local_user)
            postgres_connection.commit()


            root_logger.info("")
            root_logger.info(f"Successfully mapped the '{fdw_user}' fdw user to the '{username}' local user. ")
            root_logger.info("")

            root_logger.info("")
            root_logger.info("-------------------------------------------------------------------------------------------------------------------------------------------")
            root_logger.info("")
            root_logger.info(f"You should now be able to create and interact with the virtual tables that mirror the actual tables from the '{previous_db_name}' database. ")
            root_logger.info("")
            root_logger.info("-------------------------------------------------------------------------------------------------------------------------------------------")
            root_logger.info("")
        except Exception as e:
            print(e)



        # Import the foreign schema from the previous layer's source table 
        try:
            import_foreign_schema = f'''    IMPORT FOREIGN SCHEMA "{previous_schema_name}"
                                                LIMIT TO ({src_table_name})
                                                FROM SERVER {foreign_server}
                                                INTO {active_schema_name}
                                                ;
            '''

            cursor.execute(import_foreign_schema)
            postgres_connection.commit()

            
            root_logger.info("")
            root_logger.info(f"Successfully imported the '{src_table_name}' table into '{active_db_name}' database . ")
            root_logger.info("")

 
        except Exception as e:
            print(e)
            root_logger.error("")
            root_logger.error(f"Unable to import the '{src_table_name}' table into '{active_db_name}' database . ")
            root_logger.error("")




        # ================================================== EXTRACT DATA FROM SOURCE POSTGRES TABLE ==================================================
            
        # Extract non-data lineage columns from raw table 
        try:
            data_lineage_columns = ['created_at',    
                                    'updated_at',    
                                    'source_system', 
                                    'source_file',   
                                    'load_timestamp',
                                    'dwh_layer']
            
            desired_sql_columns = []
            

            get_list_of_column_names    =   f'''            SELECT      column_name 
                                                            FROM        information_schema.columns 
                                                            WHERE       table_name = '{src_table_name}'
                                                            ORDER BY    ordinal_position 
            '''

            cursor.execute(get_list_of_column_names)
            postgres_connection.commit()

            list_of_column_names = cursor.fetchall()
            column_names = [sql_result[0] for sql_result in list_of_column_names]
            

            total_desired_sql_columns_added = 0
            for column_name in column_names:
                if column_name not in data_lineage_columns:
                    total_desired_sql_columns_added += 1
                    desired_sql_columns.append(column_name)
                    root_logger.info(f''' {total_desired_sql_columns_added}:    Added column '{column_name}' to desired columns list...  ''')
            root_logger.info('')
            root_logger.info(f''' COMPLETED: Successfully added {total_desired_sql_columns_added}/{len(list_of_column_names)} columns to desired SQL columns list. The remaining {len(list_of_column_names)} columns not included were data lineage columns to be added later via ALTER command. ''')
            root_logger.info('')
            root_logger.info('')
            # root_logger.info(f'{desired_sql_columns}')
            
        except Exception as e:
            print(e)



        # Pull flight_schedules_tbl data from staging tables in Postgres database 
        try:
            fetch_raw_flight_schedules_tbl = f'''     SELECT { ', '.join(desired_sql_columns) } FROM {active_schema_name}.{src_table_name};  
            '''
            root_logger.debug(fetch_raw_flight_schedules_tbl)
            root_logger.info("")
            root_logger.info(f"Successfully IMPORTED the '{src_table_name}' virtual table from the '{foreign_server}' server into the '{active_schema_name}' schema for '{database}' database. Now advancing to data cleaning stage...")
            root_logger.info("")


            # Execute SQL command to interact with Postgres database
            cursor.execute(fetch_raw_flight_schedules_tbl)

            # Extract header names from cursor's description
            postgres_table_headers = [header[0] for header in cursor.description]


            # Execute script 
            postgres_table_results = cursor.fetchall()
            

            # Use Postgres results to create data frame for flight_schedules_tbl
            flight_schedules_tbl_df = pd.DataFrame(data=postgres_table_results, columns=postgres_table_headers)


            # Create temporary data frame     
            temp_df = flight_schedules_tbl_df

        except Exception as e:
            print(e)




        

        


        # # ================================================== TRANSFORM DATA FRAME  =======================================
        

        # Convert flight_date field from integer to date type (with yyyy-mm-dd)
        
        temp_df['flight_date']      =       pd.to_datetime(temp_df['flight_date'], unit='ms')


        # Create arrival_date column based on flight_date and departure_time fields 

        temp_df['departure_time']   =       pd.to_datetime(temp_df['departure_time'], format='%H:%M:%S').dt.time
        temp_df['arrival_time']     =       pd.to_datetime(temp_df['arrival_time'], format='%H:%M:%S').dt.time

        temp_df['departure_time']   =       pd.to_timedelta(temp_df['departure_time'].astype(str))
        temp_df['arrival_time']     =       pd.to_timedelta(temp_df['arrival_time'].astype(str))
        temp_df['duration']         =       abs(temp_df['arrival_time'] - temp_df['departure_time'])

    
        temp_df['duration']         =       temp_df['duration'].dt.total_seconds() / 60 / 60
        temp_df['arrival_date']     =       temp_df['flight_date'] +  pd.to_timedelta(temp_df['duration'])

        
        temp_df['flight_date']      =       pd.to_datetime(temp_df['flight_date'], unit='ms').dt.strftime('%Y-%m-%d')
        temp_df['arrival_date']     =       pd.to_datetime(temp_df['arrival_date'], unit='ms').dt.strftime('%Y-%m-%d')






        print(temp_df)
        print(temp_df.columns)
        
        # Write results to temp file for data validation checks 
        with open(f'{DATASETS_LOCATION_PATH}/temp_results.json', 'w') as temp_results_file:
            temp_results_file_df_to_json = temp_df.to_json(orient="records")
            temp_results_file.write(json.dumps(json.loads(temp_results_file_df_to_json), indent=4, sort_keys=True)) 

        


        # ================================================== LOAD RAW DATA TO STAGING TABLE =======================================
        

        # Set up SQL statements for table deletion and validation check  
        delete_stg_flight_schedules_tbl_if_exists     =   f''' DROP TABLE IF EXISTS {active_schema_name}.{table_name} CASCADE;
        '''

        check_if_stg_flight_schedules_tbl_is_deleted  =   f'''   SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}' );
        '''

        # Set up SQL statements for table creation and validation check 
        create_stg_flight_schedules_tbl = f'''                CREATE TABLE IF NOT EXISTS {active_schema_name}.{table_name} (
                                                                                    flight_id                       UUID PRIMARY KEY NOT NULL UNIQUE,
                                                                                    arrival_city                    VARCHAR NOT NULL,
                                                                                    arrival_date                    DATE NOT NULL,
                                                                                    arrival_time                    TIME NOT NULL,
                                                                                    departure_city                  VARCHAR NOT NULL,
                                                                                    departure_time                  TIME NOT NULL,
                                                                                    duration                        NUMERIC(10, 2),
                                                                                    flight_date                     DATE NOT NULL
                                                                        );
        '''

        check_if_stg_flight_schedules_tbl_exists  =   f'''       SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}' );
        '''

       



        # Set up SQL statements for adding data lineage and validation check 
        add_data_lineage_to_stg_flight_schedules_tbl  =   f'''        ALTER TABLE {active_schema_name}.{table_name}
                                                                                ADD COLUMN  created_at                  TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                                                                                ADD COLUMN  updated_at                  TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                                                                                ADD COLUMN  source_system               VARCHAR(255),
                                                                                ADD COLUMN  source_file                 VARCHAR(255),
                                                                                ADD COLUMN  load_timestamp              TIMESTAMP,
                                                                                ADD COLUMN  dwh_layer                   VARCHAR(255)
                                                                        ;
        '''

        check_if_data_lineage_fields_are_added_to_tbl   =   f'''        
                                                                    SELECT * 
                                                                    FROM    information_schema.columns 
                                                                    WHERE   table_name      = '{table_name}' 
                                                                        AND     (column_name    = 'created_at'
                                                                        OR      column_name     = 'updated_at' 
                                                                        OR      column_name     = 'source_system' 
                                                                        OR      column_name     = 'source_file' 
                                                                        OR      column_name     = 'load_timestamp' 
                                                                        OR      column_name     = 'dwh_layer');
                                                                              
        '''
        
        check_total_row_count_before_insert_statement   =   f'''   SELECT COUNT(*) FROM {active_schema_name}.{table_name}
        '''

        # Set up SQL statements for records insert and validation check
        insert_flight_schedules_data  =   f'''                       INSERT INTO {active_schema_name}.{table_name} (
                                                                                flight_id,
                                                                                arrival_city,
                                                                                arrival_date,
                                                                                arrival_time,
                                                                                departure_city,
                                                                                departure_time,
                                                                                duration,
                                                                                flight_date,
                                                                                created_at,
                                                                                updated_at,
                                                                                source_system,
                                                                                source_file,
                                                                                load_timestamp,
                                                                                dwh_layer
                                                                            )
                                                                            VALUES (
                                                                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                                                                                );
        '''

        check_total_row_count_after_insert_statement    =   f'''        SELECT COUNT(*) FROM {active_schema_name}.{table_name}
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
        cursor.execute(delete_stg_flight_schedules_tbl_if_exists)
        DELETING_SCHEMA_PROCESSING_END_TIME     =   time.time()

        
        DELETING_SCHEMA_VAL_CHECK_PROCESSING_START_TIME     =   time.time()
        cursor.execute(check_if_stg_flight_schedules_tbl_is_deleted)
        DELETING_SCHEMA_VAL_CHECK_PROCESSING_END_TIME       =   time.time()


        sql_result = cursor.fetchone()[0]
        if sql_result:
            root_logger.debug(f"")
            root_logger.info(f"=============================================================================================================================================================================")
            root_logger.info(f"TABLE DELETION SUCCESS: Managed to drop {table_name} table in {active_db_name}. Now advancing to recreating table... ")
            root_logger.info(f"SQL Query for validation check:  {check_if_stg_flight_schedules_tbl_is_deleted} ")
            root_logger.info(f"=============================================================================================================================================================================")
            root_logger.debug(f"")
        else:
            root_logger.debug(f"")
            root_logger.error(f"==========================================================================================================================================================================")
            root_logger.error(f"TABLE DELETION FAILURE: Unable to delete {table_name}. This table may have objects that depend on it (use DROP TABLE ... CASCADE to resolve) or it doesn't exist. ")
            root_logger.error(f"SQL Query for validation check:  {check_if_stg_flight_schedules_tbl_is_deleted} ")
            root_logger.error(f"==========================================================================================================================================================================")
            root_logger.debug(f"")



        # Create table if it doesn't exist in Postgres  
        CREATING_TABLE_PROCESSING_START_TIME    =   time.time()
        cursor.execute(create_stg_flight_schedules_tbl)
        CREATING_TABLE_PROCESSING_END_TIME  =   time.time()

        
        CREATING_TABLE_VAL_CHECK_PROCESSING_START_TIME  =   time.time()
        cursor.execute(check_if_stg_flight_schedules_tbl_exists)
        CREATING_TABLE_VAL_CHECK_PROCESSING_END_TIME    =   time.time()


        sql_result = cursor.fetchone()[0]
        if sql_result:
            root_logger.debug(f"")
            root_logger.info(f"=============================================================================================================================================================================")
            root_logger.info(f"TABLE CREATION SUCCESS: Managed to create {table_name} table in {active_db_name}.  ")
            root_logger.info(f"SQL Query for validation check:  {check_if_stg_flight_schedules_tbl_exists} ")
            root_logger.info(f"=============================================================================================================================================================================")
            root_logger.debug(f"")
        else:
            root_logger.debug(f"")
            root_logger.error(f"==========================================================================================================================================================================")
            root_logger.error(f"TABLE CREATION FAILURE: Unable to create {table_name}... ")
            root_logger.error(f"SQL Query for validation check:  {check_if_stg_flight_schedules_tbl_exists} ")
            root_logger.error(f"==========================================================================================================================================================================")
            root_logger.debug(f"")



        # Add data lineage to table 
        ADDING_DATA_LINEAGE_PROCESSING_START_TIME   =   time.time()
        cursor.execute(add_data_lineage_to_stg_flight_schedules_tbl)
        ADDING_DATA_LINEAGE_PROCESSING_END_TIME     =   time.time()

        
        ADDING_DATA_LINEAGE_VAL_CHECK_PROCESSING_START_TIME  =  time.time()
        cursor.execute(check_if_data_lineage_fields_are_added_to_tbl)
        ADDING_DATA_LINEAGE_VAL_CHECK_PROCESSING_END_TIME    =  time.time()


        sql_results = cursor.fetchall()
        
        if len(sql_results) == 6:
            root_logger.debug(f"")
            root_logger.info(f"=============================================================================================================================================================================")
            root_logger.info(f"DATA LINEAGE FIELDS CREATION SUCCESS: Managed to create data lineage columns in {active_schema_name}.{table_name}.  ")
            root_logger.info(f"SQL Query for validation check:  {check_if_data_lineage_fields_are_added_to_tbl} ")
            root_logger.info(f"=============================================================================================================================================================================")
            root_logger.debug(f"")
        else:
            root_logger.debug(f"")
            root_logger.error(f"==========================================================================================================================================================================")
            root_logger.error(f"DATA LINEAGE FIELDS CREATION FAILURE: Unable to create create data lineage columns in {active_schema_name}.{table_name}.... ")
            root_logger.error(f"SQL Query for validation check:  {check_if_data_lineage_fields_are_added_to_tbl} ")
            root_logger.error(f"==========================================================================================================================================================================")
            root_logger.debug(f"")



        # Add insert rows to table 
        ROW_INSERTION_PROCESSING_START_TIME     =   time.time()
        cursor.execute(check_total_row_count_before_insert_statement)
        sql_result = cursor.fetchone()[0]
        root_logger.info(f"Rows before SQL insert in Postgres: {sql_result} ")
        root_logger.debug(f"")


        for index, row in temp_df.iterrows():
            values = (
                row['flight_id'],
                row['arrival_city'],
                row['arrival_date'],
                row['arrival_time'],
                row['departure_city'],
                row['departure_time'],
                row['duration'],
                row['flight_date'],  
                CURRENT_TIMESTAMP,
                CURRENT_TIMESTAMP,
                random.choice(source_system),
                src_table_name,
                CURRENT_TIMESTAMP,
                data_warehouse_layer
                    )

            cursor.execute(insert_flight_schedules_data, values)


            # Validate if each row inserted into the table exists 
            if cursor.rowcount == 1:
                row_counter += 1
                successful_rows_upload_count += 1
                root_logger.debug(f'---------------------------------')
                root_logger.info(f'INSERT SUCCESS: Uploaded flight_schedules record no {row_counter} ')
                root_logger.debug(f'---------------------------------')
            else:
                row_counter += 1
                failed_rows_upload_count +=1
                root_logger.error(f'---------------------------------')
                root_logger.error(f'INSERT FAILED: Unable to insert flight_schedules record no {row_counter} ')
                root_logger.error(f'---------------------------------')


        
        ROW_INSERTION_PROCESSING_END_TIME   =   time.time()


        ROW_COUNT_VAL_CHECK_PROCESSING_START_TIME   =   time.time()
        cursor.execute(check_total_row_count_after_insert_statement)
        ROW_COUNT_VAL_CHECK_PROCESSING_END_TIME     =   time.time()


        total_rows_in_table = cursor.fetchone()[0]
        root_logger.info(f"Rows after SQL insert in Postgres: {total_rows_in_table} ")
        root_logger.debug(f"")



        # ======================================= SENSITIVE COLUMN IDENTIFICATION =======================================

        note_1 = """IMPORTANT NOTE: Invest time in understanding the underlying data fields to avoid highlighting the incorrect fields or omitting fields containing confidential information.          """
        note_2 = """      Involving the relevant stakeholders in the process of identifying sensitive data fields from the source data is a crucial step to protecting confidential information. """
        note_3 = """      Neglecting this step could expose customers and the wider company to serious harm (e.g. cybersecurity hacks, data breaches, unauthorized access to sensitive data), so approach this task with the utmost care. """
        
        root_logger.warning(f'')
        root_logger.warning(f'')
        root_logger.warning('================================================')
        root_logger.warning('           SENSITIVE COLUMN IDENTIFICATION              ')
        root_logger.warning('================================================')
        root_logger.warning(f'')
        root_logger.error(f'{note_1}')
        root_logger.error(f'')
        root_logger.error(f'{note_2}')
        root_logger.error(f'')
        root_logger.error(f'{note_3}')
        root_logger.warning(f'')
        root_logger.warning(f'')
        root_logger.warning(f'Now beginning the sensitive column identification stage ...')
        root_logger.warning(f'')
        

        # Add a flag for confirming if sensitive data fields have been highlighted  
        sensitive_columns_selected = [None
                            ]
        
        

        if len(sensitive_columns_selected) == 0:
            SENSITIVE_COLUMNS_IDENTIFIED = False
            root_logger.error(f"ERROR: No sensitive columns have been selected for '{table_name}' table ")
            root_logger.warning(f'')
        
        elif sensitive_columns_selected[0] is None:
            SENSITIVE_COLUMNS_IDENTIFIED = True
            root_logger.error(f"There are no sensitive columns for the '{table_name}' table ")
            root_logger.warning(f'')

        else:
            SENSITIVE_COLUMNS_IDENTIFIED = True
            root_logger.warning(f'Here are the columns considered sensitive in this table ...')
            root_logger.warning(f'')

        
        if SENSITIVE_COLUMNS_IDENTIFIED is False:
            sql_statement_for_listing_columns_in_table = f"""        
            SELECT column_name FROM information_schema.columns 
            WHERE   table_name = '{table_name}'
            ORDER BY ordinal_position 
            """
            cursor.execute(get_list_of_column_names)
            list_of_column_names = cursor.fetchall()
            column_names = [sql_result[0] for sql_result in list_of_column_names]
            
            root_logger.warning(f"You are required to select the sensitive columns in this table. If there are none, enter 'None' in the 'sensitive_columns_selected' object.")
            root_logger.warning(f'')
            root_logger.warning(f"Here are the columns to choose from:")
            root_logger.warning(f'')
            total_sensitive_columns = 0
            for sensitive_column_name in column_names:
                total_sensitive_columns += 1
                root_logger.warning(f'''{total_sensitive_columns} : '{sensitive_column_name}'  ''')



            root_logger.warning(f'')
            root_logger.warning(f'You can use this SQL query to list the columns in this table:')
            root_logger.warning(f'              {sql_statement_for_listing_columns_in_table}                ')
        
        else:
            total_sensitive_columns = 0
            for sensitive_column_name in sensitive_columns_selected:
                total_sensitive_columns += 1
                root_logger.warning(f'''{total_sensitive_columns} : '{sensitive_column_name}'  ''')
            if sensitive_columns_selected[0] is not None:
                root_logger.warning(f'')
                root_logger.warning(f'')
                root_logger.warning(f'Decide on the appropriate treatment for these tables. A few options to consider include:')
                root_logger.warning(f'''1. Masking fields               -   This involves replacing sensitive columns with alternative characters e.g.  'xxxx-xxxx', '*****', '$$$$'. ''')
                root_logger.warning(f'''2. Encrypting fields            -   This is converting sensitive columns to cipher text (unreadable text format).        ''')
                root_logger.warning(f'''3. Role-based access control    -   Placing a system that delegates privileges based on team members' responsibilities        ''')
            
            root_logger.warning(f'')
            root_logger.warning(f'Now terminating the sensitive column identification stage ...')
            root_logger.warning(f'Sensitive column identification stage ended. ')
            root_logger.warning(f'')


        root_logger.warning(f'')
        root_logger.warning(f'')





        # ======================================= DATA PROFILING METRICS =======================================


        # Prepare data profiling metrics 


        # --------- A. Table statistics 
        cursor.execute(count_total_no_of_columns_in_table)
        total_columns_in_table = cursor.fetchone()[0]

        cursor.execute(count_total_no_of_unique_records_in_table)
        total_unique_records_in_table = cursor.fetchone()[0]
        total_duplicate_records_in_table = total_rows_in_table - total_unique_records_in_table


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


        EXECUTION_TIME_FOR_ADDING_DATA_LINEAGE               =   (ADDING_DATA_LINEAGE_PROCESSING_END_TIME            -       ADDING_DATA_LINEAGE_PROCESSING_START_TIME               )   * 1000


        EXECUTION_TIME_FOR_ADDING_DATA_LINEAGE_VAL_CHECK     =   (ADDING_DATA_LINEAGE_VAL_CHECK_PROCESSING_END_TIME  -       ADDING_DATA_LINEAGE_VAL_CHECK_PROCESSING_START_TIME     )   * 1000


        EXECUTION_TIME_FOR_ROW_INSERTION                     =   (ROW_INSERTION_PROCESSING_END_TIME                  -       ROW_INSERTION_PROCESSING_START_TIME                     )   * 1000


        EXECUTION_TIME_FOR_ROW_COUNT                         =   (ROW_COUNT_VAL_CHECK_PROCESSING_END_TIME            -       ROW_COUNT_VAL_CHECK_PROCESSING_START_TIME               )   * 1000




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
        root_logger.info(f'Number of rows in table:                     {total_rows_in_table} ')
        root_logger.info(f'Number of columns in table:                  {total_columns_in_table} ')
        root_logger.info(f'')


        if successful_rows_upload_count == total_rows_in_table:
            root_logger.info(f'Successful records uploaded total :          {successful_rows_upload_count} / {total_rows_in_table}   ')
            root_logger.info(f'Failed/Errored records uploaded total:       {failed_rows_upload_count} / {total_rows_in_table}       ')
            root_logger.info(f'')
            root_logger.info(f'Successful records uploaded % :              {(successful_rows_upload_count / total_rows_in_table) * 100}    ')
            root_logger.info(f'Failed/Errored records uploaded %:           {(failed_rows_upload_count/total_rows_in_table) * 100}       ')
            root_logger.info(f'')
        else:
            root_logger.warning(f'Successful records uploaded total :          {successful_rows_upload_count} / {total_rows_in_table}   ')
            root_logger.warning(f'Failed/Errored records uploaded total:       {failed_rows_upload_count} / {total_rows_in_table}       ')
            root_logger.warning(f'')
            root_logger.warning(f'Successful records uploaded % :              {(successful_rows_upload_count / total_rows_in_table) * 100}    ')
            root_logger.warning(f'Failed/Errored records uploaded %:           {(failed_rows_upload_count/total_rows_in_table) * 100}       ')
            root_logger.warning(f'')


        if total_unique_records_in_table == total_rows_in_table:
            root_logger.info(f'Number of unique records:                    {total_unique_records_in_table} / {total_rows_in_table}')
            root_logger.info(f'Number of duplicate records:                 {total_duplicate_records_in_table} / {total_rows_in_table}')
            root_logger.info(f'')
            root_logger.info(f'Unique records %:                            {(total_unique_records_in_table / total_rows_in_table) * 100} ')
            root_logger.info(f'Duplicate records %:                         {(total_duplicate_records_in_table / total_rows_in_table)  * 100} ')
            root_logger.info(f'')
        
        else:
            root_logger.warning(f'Number of unique records:                    {total_unique_records_in_table} / {total_rows_in_table}')
            root_logger.warning(f'Number of duplicate records:                 {total_duplicate_records_in_table} / {total_rows_in_table}')
            root_logger.warning(f'')
            root_logger.warning(f'Unique records %:                            {(total_unique_records_in_table / total_rows_in_table) * 100} ')
            root_logger.warning(f'Duplicate records %:                         {(total_duplicate_records_in_table / total_rows_in_table)  * 100} ')
            root_logger.warning(f'')
        

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



        if (EXECUTION_TIME_FOR_ADDING_DATA_LINEAGE > 1000) and (EXECUTION_TIME_FOR_ADDING_DATA_LINEAGE < 60000):
            root_logger.info(f'7. Execution time for ADDING data lineage:  {EXECUTION_TIME_FOR_ADDING_DATA_LINEAGE} ms ({  round   (EXECUTION_TIME_FOR_ADDING_DATA_LINEAGE  /   1000, 2)} secs)      ')
            root_logger.info(f'')
            root_logger.info(f'')
        elif (EXECUTION_TIME_FOR_ADDING_DATA_LINEAGE >= 60000):
            root_logger.info(f'7. Execution time for ADDING data lineage:  {EXECUTION_TIME_FOR_ADDING_DATA_LINEAGE} ms ({  round   (EXECUTION_TIME_FOR_ADDING_DATA_LINEAGE  /   1000, 2)} secs)  ({  round ((EXECUTION_TIME_FOR_ADDING_DATA_LINEAGE  /   1000) / 60,  4)   } min)      ')
            root_logger.info(f'')
            root_logger.info(f'')
        else:
            root_logger.info(f'7. Execution time for ADDING data lineage:  {EXECUTION_TIME_FOR_ADDING_DATA_LINEAGE} ms ')
            root_logger.info(f'')
            root_logger.info(f'')



        if (EXECUTION_TIME_FOR_ADDING_DATA_LINEAGE_VAL_CHECK > 1000) and (EXECUTION_TIME_FOR_ADDING_DATA_LINEAGE_VAL_CHECK < 60000):
            root_logger.info(f'8. Execution time for ADDING data lineage (VAL CHECK):  {EXECUTION_TIME_FOR_ADDING_DATA_LINEAGE_VAL_CHECK} ms ({  round   (EXECUTION_TIME_FOR_ADDING_DATA_LINEAGE_VAL_CHECK  /   1000, 2)} secs)      ')
            root_logger.info(f'')
            root_logger.info(f'')
        elif (EXECUTION_TIME_FOR_ADDING_DATA_LINEAGE_VAL_CHECK >= 60000):
            root_logger.info(f'8. Execution time for ADDING data lineage (VAL CHECK):  {EXECUTION_TIME_FOR_ADDING_DATA_LINEAGE_VAL_CHECK} ms ({  round   (EXECUTION_TIME_FOR_ADDING_DATA_LINEAGE_VAL_CHECK  /   1000, 2)} secs)   ({  round ((EXECUTION_TIME_FOR_ADDING_DATA_LINEAGE_VAL_CHECK  /   1000) / 60,  4)   } min)      ')
            root_logger.info(f'')
            root_logger.info(f'')
        else:
            root_logger.info(f'8. Execution time for ADDING data lineage (VAL CHECK):  {EXECUTION_TIME_FOR_ADDING_DATA_LINEAGE_VAL_CHECK} ms ')
            root_logger.info(f'')
            root_logger.info(f'')



        if (EXECUTION_TIME_FOR_ROW_INSERTION > 1000) and (EXECUTION_TIME_FOR_ROW_INSERTION < 60000):
            root_logger.info(f'9. Execution time for INSERTING rows to table:  {EXECUTION_TIME_FOR_ROW_INSERTION} ms ({  round   (EXECUTION_TIME_FOR_ROW_INSERTION  /   1000, 2)} secs)      ')
            root_logger.info(f'')
            root_logger.info(f'')
        elif (EXECUTION_TIME_FOR_ROW_INSERTION >= 60000):
            root_logger.info(f'9. Execution time for INSERTING rows to table:  {EXECUTION_TIME_FOR_ROW_INSERTION} ms ({  round   (EXECUTION_TIME_FOR_ROW_INSERTION  /   1000, 2)} secs)   ({  round ((EXECUTION_TIME_FOR_ROW_INSERTION  /   1000) / 60,  4)   } min)      ')
            root_logger.info(f'')
            root_logger.info(f'')
        else:
            root_logger.info(f'9. Execution time for INSERTING rows to table:  {EXECUTION_TIME_FOR_ROW_INSERTION} ms ')
            root_logger.info(f'')
            root_logger.info(f'')



        if (EXECUTION_TIME_FOR_ROW_COUNT > 1000) and (EXECUTION_TIME_FOR_ROW_COUNT < 60000):
            root_logger.info(f'10. Execution time for COUNTING uploaded rows to table:  {EXECUTION_TIME_FOR_ROW_COUNT} ms ({  round   (EXECUTION_TIME_FOR_ROW_COUNT  /   1000, 2)} secs)      ')
            root_logger.info(f'')
            root_logger.info(f'')
        elif (EXECUTION_TIME_FOR_ROW_COUNT >= 60000):
            root_logger.info(f'10. Execution time for COUNTING uploaded rows to table:  {EXECUTION_TIME_FOR_ROW_COUNT} ms ({  round   (EXECUTION_TIME_FOR_ROW_COUNT  /   1000, 2)} secs)    ({  round ((EXECUTION_TIME_FOR_ROW_COUNT  /   1000) / 60,  4)   } min)      ')
            root_logger.info(f'')
            root_logger.info(f'')
        else:
            root_logger.info(f'10. Execution time for COUNTING uploaded rows to table:  {EXECUTION_TIME_FOR_ROW_COUNT} ms ')
            root_logger.info(f'')
            root_logger.info(f'')



        root_logger.info(f'')
        root_logger.info('================================================')


        # Add conditional statements for data profile metrics 

        if successful_rows_upload_count != total_rows_in_table:
            if successful_rows_upload_count == 0:
                root_logger.error(f"ERROR: No records were upload to '{table_name}' table....")
                raise ImportError("Trace filepath to highlight the root cause of the missing rows...")
            else:
                root_logger.error(f"ERROR: There are only {successful_rows_upload_count} records upload to '{table_name}' table....")
                raise ImportError("Trace filepath to highlight the root cause of the missing rows...")
        

        elif failed_rows_upload_count > 0:
            root_logger.error(f"ERROR: A total of {failed_rows_upload_count} records failed to upload to '{table_name}' table....")
            raise ImportError("Trace filepath to highlight the root cause of the missing rows...")
        

        elif total_unique_records_in_table != total_rows_in_table:
            root_logger.error(f"ERROR: There are {total_duplicate_records_in_table} duplicated records in the uploads for '{table_name}' table....")
            raise ImportError("Trace filepath to highlight the root cause of the duplicated rows...")


        elif total_duplicate_records_in_table > 0:
            root_logger.error(f"ERROR: There are {total_duplicate_records_in_table} duplicated records in the uploads for '{table_name}' table....")
            raise ImportError("Trace filepath to highlight the root cause of the duplicated rows...")
        

        elif total_null_values_in_table > 0:
            root_logger.error(f"ERROR: There are {total_duplicate_records_in_table} NULL values in '{table_name}' table....")
            raise ImportError("Examine table to highlight the columns with the NULL values - justify if these fields should contain NULLs ...")

    

        else:
            root_logger.debug("")
            root_logger.info("DATA VALIDATION SUCCESS: All general DQ checks passed! ")
            root_logger.debug("")





        # Commit the changes made in Postgres 
        root_logger.info("Now saving changes made by SQL statements to Postgres DB....")
        postgres_connection.commit()
        root_logger.info("Saved successfully, now terminating cursor and current session....")


    except Exception as e:
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
            # root_logger.debug("")
            root_logger.debug("Session connected to Postgres database closed.")



load_data_to_stg_flight_schedules_table(postgres_connection)

