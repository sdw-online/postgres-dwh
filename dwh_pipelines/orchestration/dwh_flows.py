from prefect import task, flow
import logging, coloredlogs
from pathlib import Path

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
file_handler        =   logging.FileHandler('logs/orchestration/' + current_filepath + '.log', mode='w')
file_handler.setFormatter(file_handler_log_formatter)


# Set up console handler object for writing event logs to console in real time (i.e. streams events to stderr)
console_handler     =   logging.StreamHandler()
console_handler.setFormatter(console_handler_log_formatter)


# Add the file and console handlers 
root_logger.addHandler(file_handler)


# Only add the console handler if the script is running directly from this location 
if __name__=="__main__":
    root_logger.addHandler(console_handler)







# ============================================== 0. DATA GENERATION ==============================================
# Set up source data generation task

@task
def generate_synthetic_travel_data():
    from dwh_pipelines.L0_src_data_generator.src_data_generator import  generate_travel_data
    module_name = 'dwh_pipelines.L0_src_data_generator.src_data_generator'
    imported_function = 'generate_travel_data'
    root_logger.info("")
    root_logger.info(f"Now importing '{imported_function}' function from  '{module_name}' module")
    root_logger.info("")




# # ============================================== 1. RAW LAYER ============================================== 

# Set up tasks for raw layer

@task
def load_data_to_raw_accommodation_bookings_tbl():
    from dwh_pipelines.L1_raw_layer.raw_accommodation_bookings_tbl      import  load_accommodation_bookings_data_to_raw_table
    module_name = 'dwh_pipelines.L1_raw_layer.raw_accommodation_bookings_tbl'
    imported_function = 'load_accommodation_bookings_data_to_raw_table'
    root_logger.info("")
    root_logger.info(f"Now importing '{imported_function}' function from  '{module_name}' module")
    root_logger.info("")



@task
def load_data_to_raw_customer_feedbacks_tbl():
    from dwh_pipelines.L1_raw_layer.raw_customer_feedbacks_tbl          import  load_customer_feedbacks_data_to_raw_table
    module_name = 'dwh_pipelines.L1_raw_layer.raw_customer_feedbacks_tbl'
    imported_function = 'load_customer_feedbacks_data_to_raw_table'
    root_logger.info("")
    root_logger.info(f"Now importing '{imported_function}' function from  '{module_name}' module")
    root_logger.info("")



@task
def load_data_to_raw_customer_info_tbl():
    from dwh_pipelines.L1_raw_layer.raw_customer_info_tbl               import  load_customer_info_data_to_raw_table
    module_name = 'dwh_pipelines.L1_raw_layer.raw_customer_info_tbl'
    imported_function = 'load_customer_info_data_to_raw_table'
    root_logger.info("")
    root_logger.info(f"Now importing '{imported_function}' function from  '{module_name}' module")
    root_logger.info("")



@task
def load_data_to_raw_flight_bookings_tbl():
    from dwh_pipelines.L1_raw_layer.raw_flight_bookings_tbl             import  load_flight_bookings_data_to_raw_table
    module_name = 'dwh_pipelines.L1_raw_layer.raw_flight_bookings_tbl'
    imported_function = 'load_flight_bookings_data_to_raw_table'
    root_logger.info("")
    root_logger.info(f"Now importing '{imported_function}' function from  '{module_name}' module")
    root_logger.info("")



@task
def load_data_to_raw_flight_destinations_tbl():
    from dwh_pipelines.L1_raw_layer.raw_flight_destinations_tbl         import  load_flight_destinations_data_to_raw_table
    module_name = 'dwh_pipelines.L1_raw_layer.raw_flight_destinations_tbl'
    imported_function = 'load_flight_destinations_data_to_raw_table'
    root_logger.info("")
    root_logger.info(f"Now importing '{imported_function}' function from  '{module_name}' module")
    root_logger.info("")



@task
def load_data_to_raw_flight_promotion_deals_tbl():
    from dwh_pipelines.L1_raw_layer.raw_flight_promotion_deals_tbl      import  load_flight_promotion_deals_data_to_raw_table
    module_name = 'dwh_pipelines.L1_raw_layer.raw_flight_promotion_deals_tbl'
    imported_function = 'load_flight_promotion_deals_data_to_raw_table'
    root_logger.info("")
    root_logger.info(f"Now importing '{imported_function}' function from  '{module_name}' module")
    root_logger.info("")



@task
def load_data_to_raw_flight_schedules_tbl():
    from dwh_pipelines.L1_raw_layer.raw_flight_schedules_tbl            import  load_flight_schedules_data_to_raw_table
    module_name = 'dwh_pipelines.L1_raw_layer.raw_flight_schedules_tbl'
    imported_function = 'load_flight_schedules_data_to_raw_table'
    root_logger.info("")
    root_logger.info(f"Now importing '{imported_function}' function from  '{module_name}' module")
    root_logger.info("")



@task
def load_data_to_raw_flight_ticket_sales_tbl():
    from dwh_pipelines.L1_raw_layer.raw_flight_ticket_sales_tbl         import  load_flight_ticket_sales_data_to_raw_table
    module_name = 'dwh_pipelines.L1_raw_layer.raw_flight_ticket_sales_tbl'
    imported_function = 'load_flight_ticket_sales_data_to_raw_table'
    root_logger.info("")
    root_logger.info(f"Now importing '{imported_function}' function from  '{module_name}' module")
    root_logger.info("")



@task
def load_data_to_raw_sales_agents_tbl():
    from dwh_pipelines.L1_raw_layer.raw_sales_agents_tbl                import  load_sales_agents_data_to_raw_table
    module_name = 'dwh_pipelines.L1_raw_layer.raw_sales_agents_tbl'
    imported_function = 'load_sales_agents_data_to_raw_table'
    root_logger.info("")
    root_logger.info(f"Now importing '{imported_function}' function from  '{module_name}' module")
    root_logger.info("")



@task
def load_data_to_raw_ticket_prices_tbl():
    from dwh_pipelines.L1_raw_layer.raw_ticket_prices_tbl               import  load_ticket_prices_data_to_raw_table
    module_name = 'dwh_pipelines.L1_raw_layer.raw_ticket_prices_tbl'
    imported_function = 'load_ticket_prices_data_to_raw_table'
    root_logger.info("")
    root_logger.info(f"Now importing '{imported_function}' function from  '{module_name}' module")
    root_logger.info("")




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