from prefect import task, flow










# ============================================== 0. DATA GENERATION ==============================================
# Set up source data generation task

@task
def generate_synthetic_travel_data():
    from dwh_pipelines.L0_src_data_generator.src_data_generator import  generate_travel_data




# # ============================================== 1. RAW LAYER ============================================== 

# # Set up tasks for raw layer

@task
def load_data_to_raw_accommodation_bookings_tbl():
    from dwh_pipelines.L1_raw_layer.raw_accommodation_bookings_tbl      import  load_accommodation_bookings_data_to_raw_table


@task
def load_data_to_raw_customer_feedbacks_tbl():
    from dwh_pipelines.L1_raw_layer.raw_customer_feedbacks_tbl          import  load_customer_feedbacks_data_to_raw_table


@task
def load_data_to_raw_customer_info_tbl():
    from dwh_pipelines.L1_raw_layer.raw_customer_info_tbl               import  load_customer_info_data_to_raw_table



@task
def load_data_to_raw_flight_bookings_tbl():
    from dwh_pipelines.L1_raw_layer.raw_flight_bookings_tbl             import  load_flight_bookings_data_to_raw_table


@task
def load_data_to_raw_flight_destinations_tbl():
    from dwh_pipelines.L1_raw_layer.raw_flight_destinations_tbl         import  load_flight_destinations_data_to_raw_table


@task
def load_data_to_raw_flight_promotion_deals_tbl():
    from dwh_pipelines.L1_raw_layer.raw_flight_promotion_deals_tbl      import  load_flight_promotion_deals_data_to_raw_table

@task
def load_data_to_raw_flight_schedules_tbl():
    from dwh_pipelines.L1_raw_layer.raw_flight_schedules_tbl            import  load_flight_schedules_data_to_raw_table


@task
def load_data_to_raw_flight_ticket_sales_tbl():
    from dwh_pipelines.L1_raw_layer.raw_flight_ticket_sales_tbl         import  load_flight_ticket_sales_data_to_raw_table


@task
def load_data_to_raw_sales_agents_tbl():
    from dwh_pipelines.L1_raw_layer.raw_sales_agents_tbl                import  load_sales_agents_data_to_raw_table


@task
def load_data_to_raw_ticket_prices_tbl():
    from dwh_pipelines.L1_raw_layer.raw_ticket_prices_tbl               import  load_ticket_prices_data_to_raw_table




# Set up sub-flow for generating travel data 
@flow(name="Generate travel data", flow_run_name="generate_travel_data_flow")
def generate_source_data_flow():
    return generate_synthetic_travel_data()




# Set up sub-flow for executing tasks in raw layer 
@flow(name="Execute tasks in raw layer", flow_run_name="source_to_raw_layer_flow")
def run_raw_layer_flow():
    load_data_to_raw_accommodation_bookings_tbl()
    load_data_to_raw_customer_feedbacks_tbl()
    load_data_to_raw_customer_feedbacks_tbl()
    load_data_to_raw_customer_info_tbl()
    load_data_to_raw_flight_bookings_tbl()
    load_data_to_raw_flight_destinations_tbl()
    load_data_to_raw_flight_promotion_deals_tbl()
    load_data_to_raw_flight_schedules_tbl()
    load_data_to_raw_flight_ticket_sales_tbl()
    load_data_to_raw_sales_agents_tbl()
    load_data_to_raw_ticket_prices_tbl()




# # Set up sub-flow dependencies 
# @flow(name="Run data warehouse tasks")
# def run_main_dwh_flow():
#     L0_GENERATE_DATA_FLOW   =   generate_source_data_flow()
#     L1_RAW_LAYER_FLOW       =   run_raw_layer_flow()


#     # L1_RAW_LAYER_FLOW.set_upstream(L0_GENERATE_DATA_FLOW)
#     L0_GENERATE_DATA_FLOW.set_upstream(L1_RAW_LAYER_FLOW)
    
# generate_source_data_flow().run(run_name="generate_data")

generate_source_data_flow()
run_raw_layer_flow()


# run_main_dwh_flow()