from prefect import task, Flow
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



# Set up source data generation task

@task
def generate_synthetic_travel_data():
    generate_travel_data()


# Set up raw layer tasks 


@task
def load_data_to_raw_accommodation_bookings_tbl():
    load_accommodation_bookings_data_to_raw_table()


@task
def load_data_to_raw_customer_feedbacks_tbl():
    load_customer_feedbacks_data_to_raw_table()


@task
def load_data_to_raw_customer_info_tbl():
    load_customer_info_data_to_raw_table()



@task
def load_data_to_raw_flight_bookings_tbl():
    load_flight_bookings_data_to_raw_table()


@task
def load_data_to_raw_flight_destinations_tbl():
    load_flight_destinations_data_to_raw_table()


@task
def load_data_to_raw_flight_promotion_deals_tbl():
    load_flight_promotion_deals_data_to_raw_table()


@task
def load_data_to_raw_flight_schedules_tbl():
    load_flight_schedules_data_to_raw_table()


@task
def load_data_to_raw_flight_ticket_sales_tbl():
    load_flight_ticket_sales_data_to_raw_table()


@task
def load_data_to_raw_sales_agents_tbl():
    load_sales_agents_data_to_raw_table()


@task
def load_data_to_raw_ticket_prices_tbl():
    load_ticket_prices_data_to_raw_table()






with Flow("Generate travel data") as L0_GENERATE_DATA_FLOW:
    
    # Create sub-flow for 
    generate_source_data = generate_synthetic_travel_data()


with Flow("Execute tasks in raw layer") as L1_RAW_LAYER_FLOW:
    load_source_data_to_raw_accommodation_bookings_tbl     =   load_data_to_raw_accommodation_bookings_tbl()
    load_source_data_to_raw_customer_feedbacks_tbl         =   load_data_to_raw_customer_feedbacks_tbl()
    load_source_data_to_raw_customer_info_tbl              =   load_data_to_raw_customer_info_tbl()
    load_source_data_to_raw_flight_bookings_tbl            =   load_data_to_raw_flight_bookings_tbl()
    load_source_data_to_raw_flight_destinations_tbl        =   load_data_to_raw_flight_destinations_tbl()
    load_source_data_to_raw_flight_promotion_deals_tbl     =   load_data_to_raw_flight_promotion_deals_tbl()
    load_source_data_to_raw_flight_schedules_tbl           =   load_data_to_raw_flight_schedules_tbl()
    load_source_data_to_raw_flight_ticket_sales_tbl        =   load_data_to_raw_flight_ticket_sales_tbl()
    load_source_data_to_raw_flight_sales_agents_tbl        =   load_data_to_raw_sales_agents_tbl()
    load_source_data_to_raw_flight_ticket_prices_tbl       =   load_data_to_raw_ticket_prices_tbl()


L1_RAW_LAYER_FLOW.set_upstream(L0_GENERATE_DATA_FLOW)
    
        
