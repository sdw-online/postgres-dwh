from prefect import task, flow, get_run_logger
from prefect.deployments import Deployment
from prefect.orion.schemas.schedules import IntervalSchedule



# ============================================== 0. DATA GENERATION ==============================================
# Set up source data generation task

@task
def generate_synthetic_travel_data():
    from dwh_pipelines.L0_src_data_generator.src_data_generator import  generate_travel_data
    module_name = 'dwh_pipelines.L0_src_data_generator.src_data_generator'
    imported_function = 'generate_travel_data'
    get_run_logger().info("")
    get_run_logger().info(f"Now importing '{imported_function}' function from '{module_name}' module...")
    get_run_logger().info("")




# # ============================================== 1. RAW LAYER ============================================== 

# Set up tasks for raw layer

@task
def load_data_to_raw_accommodation_bookings_tbl():
    from dwh_pipelines.L1_raw_layer.raw_accommodation_bookings_tbl      import  load_accommodation_bookings_data_to_raw_table
    module_name = 'dwh_pipelines.L1_raw_layer.raw_accommodation_bookings_tbl'
    imported_function = 'load_accommodation_bookings_data_to_raw_table'
    get_run_logger().info("")
    get_run_logger().info(f"Now importing '{imported_function}' function from '{module_name}' module...")
    get_run_logger().info("")



@task
def load_data_to_raw_customer_feedbacks_tbl():
    from dwh_pipelines.L1_raw_layer.raw_customer_feedbacks_tbl          import  load_customer_feedbacks_data_to_raw_table
    module_name = 'dwh_pipelines.L1_raw_layer.raw_customer_feedbacks_tbl'
    imported_function = 'load_customer_feedbacks_data_to_raw_table'
    get_run_logger().info("")
    get_run_logger().info(f"Now importing '{imported_function}' function from '{module_name}' module...")
    get_run_logger().info("")



@task
def load_data_to_raw_customer_info_tbl():
    from dwh_pipelines.L1_raw_layer.raw_customer_info_tbl               import  load_customer_info_data_to_raw_table
    module_name = 'dwh_pipelines.L1_raw_layer.raw_customer_info_tbl'
    imported_function = 'load_customer_info_data_to_raw_table'
    get_run_logger().info("")
    get_run_logger().info(f"Now importing '{imported_function}' function from '{module_name}' module...")
    get_run_logger().info("")



@task
def load_data_to_raw_flight_bookings_tbl():
    from dwh_pipelines.L1_raw_layer.raw_flight_bookings_tbl             import  load_flight_bookings_data_to_raw_table
    module_name = 'dwh_pipelines.L1_raw_layer.raw_flight_bookings_tbl'
    imported_function = 'load_flight_bookings_data_to_raw_table'
    get_run_logger().info("")
    get_run_logger().info(f"Now importing '{imported_function}' function from '{module_name}' module...")
    get_run_logger().info("")



@task
def load_data_to_raw_flight_destinations_tbl():
    from dwh_pipelines.L1_raw_layer.raw_flight_destinations_tbl         import  load_flight_destinations_data_to_raw_table
    module_name = 'dwh_pipelines.L1_raw_layer.raw_flight_destinations_tbl'
    imported_function = 'load_flight_destinations_data_to_raw_table'
    get_run_logger().info("")
    get_run_logger().info(f"Now importing '{imported_function}' function from '{module_name}' module...")
    get_run_logger().info("")



@task
def load_data_to_raw_flight_promotion_deals_tbl():
    from dwh_pipelines.L1_raw_layer.raw_flight_promotion_deals_tbl      import  load_flight_promotion_deals_data_to_raw_table
    module_name = 'dwh_pipelines.L1_raw_layer.raw_flight_promotion_deals_tbl'
    imported_function = 'load_flight_promotion_deals_data_to_raw_table'
    get_run_logger().info("")
    get_run_logger().info(f"Now importing '{imported_function}' function from '{module_name}' module...")
    get_run_logger().info("")



@task
def load_data_to_raw_flight_schedules_tbl():
    from dwh_pipelines.L1_raw_layer.raw_flight_schedules_tbl            import  load_flight_schedules_data_to_raw_table
    module_name = 'dwh_pipelines.L1_raw_layer.raw_flight_schedules_tbl'
    imported_function = 'load_flight_schedules_data_to_raw_table'
    get_run_logger().info("")
    get_run_logger().info(f"Now importing '{imported_function}' function from '{module_name}' module...")
    get_run_logger().info("")



@task
def load_data_to_raw_flight_ticket_sales_tbl():
    from dwh_pipelines.L1_raw_layer.raw_flight_ticket_sales_tbl         import  load_flight_ticket_sales_data_to_raw_table
    module_name = 'dwh_pipelines.L1_raw_layer.raw_flight_ticket_sales_tbl'
    imported_function = 'load_flight_ticket_sales_data_to_raw_table'
    get_run_logger().info("")
    get_run_logger().info(f"Now importing '{imported_function}' function from '{module_name}' module...")
    get_run_logger().info("")



@task
def load_data_to_raw_sales_agents_tbl():
    from dwh_pipelines.L1_raw_layer.raw_sales_agents_tbl                import  load_sales_agents_data_to_raw_table
    module_name = 'dwh_pipelines.L1_raw_layer.raw_sales_agents_tbl'
    imported_function = 'load_sales_agents_data_to_raw_table'
    get_run_logger().info("")
    get_run_logger().info(f"Now importing '{imported_function}' function from '{module_name}' module...")
    get_run_logger().info("")



@task
def load_data_to_raw_ticket_prices_tbl():
    from dwh_pipelines.L1_raw_layer.raw_ticket_prices_tbl               import  load_ticket_prices_data_to_raw_table
    module_name = 'dwh_pipelines.L1_raw_layer.raw_ticket_prices_tbl'
    imported_function = 'load_ticket_prices_data_to_raw_table'
    get_run_logger().info("")
    get_run_logger().info(f"Now importing '{imported_function}' function from '{module_name}' module...")
    get_run_logger().info("")




# Set up sub-flow for generating travel data 
@flow(name="Generate travel data", flow_run_name="generate_travel_data_flow")
def generate_source_data_flow():
    return generate_synthetic_travel_data()




# Set up sub-flow for executing tasks in raw layer 
@flow(name="Execute tasks in raw layer", flow_run_name="source_to_raw_layer_flow")
def run_raw_layer_flow():

    load_data_to_raw_accommodation_bookings_tbl()
    get_run_logger().info("SUCCESS! Completed loading source data into 'raw_accommodation_bookings_tbl'! ")



    load_data_to_raw_customer_feedbacks_tbl()
    get_run_logger().info("SUCCESS! Completed loading source data into 'raw_customer_feedbacks_tbl'! ")



    load_data_to_raw_customer_info_tbl()
    get_run_logger().info("SUCCESS! Completed loading source data into 'raw_customer_info_tbl'! ")



    load_data_to_raw_flight_bookings_tbl()
    get_run_logger().info("SUCCESS! Completed loading source data into 'raw_flight_bookings_tbl'! ")



    load_data_to_raw_flight_destinations_tbl()
    get_run_logger().info("SUCCESS! Completed loading source data into 'raw_flight_destinations_tbl'! ")



    load_data_to_raw_flight_promotion_deals_tbl()
    get_run_logger().info("SUCCESS! Completed loading source data into 'raw_flight_promotion_deals_tbl'! ")



    load_data_to_raw_flight_schedules_tbl()
    get_run_logger().info("SUCCESS! Completed loading source data into 'raw_flight_schedules_tbl'! ")



    load_data_to_raw_flight_ticket_sales_tbl()
    get_run_logger().info("SUCCESS! Completed loading source data into 'raw_flight_ticket_sales_tbl'! ")



    load_data_to_raw_sales_agents_tbl()
    get_run_logger().info("SUCCESS! Completed loading source data into 'raw_sales_agents_tbl'! ")



    load_data_to_raw_ticket_prices_tbl()
    get_run_logger().info("SUCCESS! Completed loading source data into 'raw_ticket_prices_tbl'! ")


    get_run_logger().info("Now terminating session for raw layer tasks...")
    get_run_logger().info("")
    get_run_logger().info("Raw tables processing session ended.")





# Specify flow execution order in DAG-less manner  
if __name__=="__main__":
    generate_source_data_flow()
    run_raw_layer_flow()
