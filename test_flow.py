from prefect import task, flow
import psycopg2
import configparser
import os

from dwh_pipelines.L0_src_data_generator.src_data_generator         import  generate_travel_data
from dwh_pipelines.L1_raw_layer.raw_accommodation_bookings_tbl      import  load_accommodation_bookings_data_to_raw_table
from dwh_pipelines.L1_raw_layer.raw_customer_feedbacks_tbl          import  load_customer_feedbacks_data_to_raw_table
from dwh_pipelines.L1_raw_layer.raw_customer_info_tbl               import  load_customer_info_data_to_raw_table
from dwh_pipelines.L1_raw_layer.raw_flight_bookings_tbl             import  load_flight_bookings_data_to_raw_table
from dwh_pipelines.L1_raw_layer.raw_flight_destinations_tbl         import  load_flight_destinations_data_to_raw_table
from dwh_pipelines.L1_raw_layer.raw_flight_promotion_deals_tbl      import  load_flight_promotion_deals_data_to_raw_table
from dwh_pipelines.L1_raw_layer.raw_flight_schedules_tbl            import  load_flight_schedules_data_to_raw_table
from dwh_pipelines.L1_raw_layer.raw_flight_ticket_sales_tbl         import  load_flight_ticket_sales_data_to_raw_table
from dwh_pipelines.L1_raw_layer.raw_sales_agents_tbl                import  load_sales_agents_data_to_raw_table
from dwh_pipelines.L1_raw_layer.raw_ticket_prices_tbl               import  load_ticket_prices_data_to_raw_table


# ================================================ CONFIG ================================================

# Add a flag/switch indicating whether Airflow is in use or not 
USING_AIRFLOW   =   False


# Create a config file for storing environment variables
config  =   configparser.ConfigParser()


# Use the local config file from the local machine 
path    =   os.path.abspath('dwh_pipelines/local_config.ini')
config.read(path)

host                    =   config['travel_data_filepath']['HOST']
port                    =   config['travel_data_filepath']['PORT']
database                =   config['travel_data_filepath']['RAW_DB']
username                =   config['travel_data_filepath']['USERNAME']
password                =   config['travel_data_filepath']['PASSWORD']

postgres_connection     =   None
cursor                  =   None



postgres_connection = psycopg2.connect(
                host        =   host,
                port        =   port,
                dbname      =   database,
                user        =   username,
                password    =   password,
        )


# ============================================== 0. DATA GENERATION ==============================================
# Set up source data generation task

@task
def generate_synthetic_travel_data():
    generate_travel_data()




# ============================================== 1. RAW LAYER ============================================== 

# Set up tasks for raw layer

@task
def load_data_to_raw_accommodation_bookings_tbl(postgres_connection):
    load_accommodation_bookings_data_to_raw_table(postgres_connection)


@task
def load_data_to_raw_customer_feedbacks_tbl(postgres_connection):
    load_customer_feedbacks_data_to_raw_table(postgres_connection)


@task
def load_data_to_raw_customer_info_tbl(postgres_connection):
    load_customer_info_data_to_raw_table(postgres_connection)



@task
def load_data_to_raw_flight_bookings_tbl(postgres_connection):
    load_flight_bookings_data_to_raw_table(postgres_connection)


@task
def load_data_to_raw_flight_destinations_tbl(postgres_connection):
    load_flight_destinations_data_to_raw_table(postgres_connection)


@task
def load_data_to_raw_flight_promotion_deals_tbl(postgres_connection):
    load_flight_promotion_deals_data_to_raw_table(postgres_connection)


@task
def load_data_to_raw_flight_schedules_tbl(postgres_connection):
    load_flight_schedules_data_to_raw_table(postgres_connection)


@task
def load_data_to_raw_flight_ticket_sales_tbl(postgres_connection):
    load_flight_ticket_sales_data_to_raw_table(postgres_connection)


@task
def load_data_to_raw_sales_agents_tbl(postgres_connection):
    load_sales_agents_data_to_raw_table(postgres_connection)


@task
def load_data_to_raw_ticket_prices_tbl(postgres_connection):
    load_ticket_prices_data_to_raw_table(postgres_connection)




# Set up sub-flow for generating travel data 
@flow(name="Generate travel data")
def generate_source_data_flow():
    generate_source_data = generate_synthetic_travel_data()




# Set up sub-flow for executing tasks in raw layer 
@flow(name="Execute tasks in raw layer")
def run_raw_layer_flow():
    
    load_source_data_to_raw_accommodation_bookings_tbl     =   load_data_to_raw_accommodation_bookings_tbl(postgres_connection)
    load_source_data_to_raw_customer_feedbacks_tbl         =   load_data_to_raw_customer_feedbacks_tbl(postgres_connection)
    load_source_data_to_raw_customer_info_tbl              =   load_data_to_raw_customer_info_tbl(postgres_connection)
    load_source_data_to_raw_flight_bookings_tbl            =   load_data_to_raw_flight_bookings_tbl(postgres_connection)
    load_source_data_to_raw_flight_destinations_tbl        =   load_data_to_raw_flight_destinations_tbl(postgres_connection)
    load_source_data_to_raw_flight_promotion_deals_tbl     =   load_data_to_raw_flight_promotion_deals_tbl(postgres_connection)
    load_source_data_to_raw_flight_schedules_tbl           =   load_data_to_raw_flight_schedules_tbl(postgres_connection)
    load_source_data_to_raw_flight_ticket_sales_tbl        =   load_data_to_raw_flight_ticket_sales_tbl(postgres_connection)
    load_source_data_to_raw_flight_sales_agents_tbl        =   load_data_to_raw_sales_agents_tbl(postgres_connection)
    load_source_data_to_raw_flight_ticket_prices_tbl       =   load_data_to_raw_ticket_prices_tbl(postgres_connection)



# Set up sub-flow dependencies 
@flow(name="Run data warehouse tasks")
def run_main_dwh_flow():
    L0_GENERATE_DATA_FLOW   =   generate_source_data_flow()
    L1_RAW_LAYER_FLOW       =   run_raw_layer_flow()


    L1_RAW_LAYER_FLOW.set_upstream(L0_GENERATE_DATA_FLOW)
    

run_main_dwh_flow._run()
# run_main_dwh_flow.visualize()