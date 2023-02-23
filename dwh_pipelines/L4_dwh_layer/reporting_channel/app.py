import os 
import time 
import psycopg2
import configparser
from pathlib import Path
import logging, coloredlogs
import pandas as pd
import dash 
import dash_core_components as dcc
import dash_html_components as html
import plotly.express as px 



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




def render_dash_visualizations(postgres_connection):
    try:
        
        # Set up constants
        
        active_schema_name                  =      'reporting'
        active_db_name                      =       database
        sql_query_1                         =      f'SELECT * FROM {active_schema_name}.avg_ticket_prices_by_year'
        sql_query_2                         =      f'SELECT * FROM {active_schema_name}.ticket_sales_by_age'
        sql_query_3                         =      f'SELECT * FROM {active_schema_name}.top_destinations'
        sql_query_4                         =      f'SELECT * FROM {active_schema_name}.total_sales_by_destination'
        data_warehouse_layer                =      'DWH - UAL'
        column_index                        =      0 
        total_null_values_in_table          =      0 
        


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
        


        # ================================================== CREATE DASHBOARD VIA PLOTLY-DASH =======================================
        

        avg_ticket_prices_by_year_df                =       pd.read_sql(sql_query_1, postgres_connection)
        ticket_sales_by_age_df                      =       pd.read_sql(sql_query_2, postgres_connection)
        top_destinations_df                         =       pd.read_sql(sql_query_3, postgres_connection)
        total_sales_by_destination_df               =       pd.read_sql(sql_query_4, postgres_connection)

        # Commit the changes made in Postgres 
        postgres_connection.commit()



        app = dash.Dash(__name__)


        app.layout = html.Div([
            html.H1("Flight Booking Data"),

            html.Div([
                dcc.Graph(
                    figure=px.scatter(avg_ticket_prices_by_year_df, x="booking_year", y="arrival_city", size="avg_ticket_price", color="arrival_city", title="Average Ticket Prices by Destination and Year")
                    )
            ]),

            html.Div([
                dcc.Graph(
                    figure=px.bar(ticket_sales_by_age_df, x="age", y="no_of_bookings", title="Number of Bookings by Age")
                    )            
            ]),

            html.Div([
                dcc.Graph(
                    figure=px.bar(top_destinations_df, x="destination", y="no_of_bookings", title="Top 10 Most Booked Destinations")
                    )
            ]),

            html.Div([
                dcc.Graph(
                    figure=px.scatter(total_sales_by_destination_df, x="booking_year", y="arrival_city", size="total_sales", color="arrival_city", title="Total Sales by Destination and Year")
                    )
                ])
        ])






        root_logger.info(f'')
        root_logger.info('================================================')



        # Commit the changes made in Postgres 
        # postgres_connection.commit()
        root_logger.info("Now rendering Dash app....")


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


    
    return app



render_dash_visualizations(postgres_connection)

