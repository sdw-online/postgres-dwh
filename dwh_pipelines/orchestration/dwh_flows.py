from prefect import task, flow, get_run_logger
from prefect.deployments import Deployment
from prefect.orion.schemas.schedules import IntervalSchedule
import logging, coloredlogs
from pathlib import Path
import subprocess
import os 


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





# ============================================== TASKS ==============================================
# ====================================================================================================




# ============================================== 0. DATA GENERATION ==============================================
# Set up source data generation task

@task
def generate_synthetic_travel_data():
    from dwh_pipelines.L0_src_data_generator.src_data_generator import  generate_travel_data
    module_name = 'dwh_pipelines.L0_src_data_generator.src_data_generator'
    imported_function = 'generate_travel_data'
    root_logger.info("")
    root_logger.info(f"Now importing '{imported_function}' function from '{module_name}' module...")
    root_logger.info("")
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
    root_logger.info("")
    root_logger.info(f"Now importing '{imported_function}' function from '{module_name}' module...")
    root_logger.info("")
    get_run_logger().info("")
    get_run_logger().info(f"Now importing '{imported_function}' function from '{module_name}' module...")
    get_run_logger().info("")



@task
def load_data_to_raw_customer_feedbacks_tbl():
    from dwh_pipelines.L1_raw_layer.raw_customer_feedbacks_tbl          import  load_customer_feedbacks_data_to_raw_table
    module_name = 'dwh_pipelines.L1_raw_layer.raw_customer_feedbacks_tbl'
    imported_function = 'load_customer_feedbacks_data_to_raw_table'
    root_logger.info("")
    root_logger.info(f"Now importing '{imported_function}' function from '{module_name}' module...")
    root_logger.info("")
    get_run_logger().info("")
    get_run_logger().info(f"Now importing '{imported_function}' function from '{module_name}' module...")
    get_run_logger().info("")



@task
def load_data_to_raw_customer_info_tbl():
    from dwh_pipelines.L1_raw_layer.raw_customer_info_tbl               import  load_customer_info_data_to_raw_table
    module_name = 'dwh_pipelines.L1_raw_layer.raw_customer_info_tbl'
    imported_function = 'load_customer_info_data_to_raw_table'
    root_logger.info("")
    root_logger.info(f"Now importing '{imported_function}' function from '{module_name}' module...")
    root_logger.info("")
    get_run_logger().info("")
    get_run_logger().info(f"Now importing '{imported_function}' function from '{module_name}' module...")
    get_run_logger().info("")



@task
def load_data_to_raw_flight_bookings_tbl():
    from dwh_pipelines.L1_raw_layer.raw_flight_bookings_tbl             import  load_flight_bookings_data_to_raw_table
    module_name = 'dwh_pipelines.L1_raw_layer.raw_flight_bookings_tbl'
    imported_function = 'load_flight_bookings_data_to_raw_table'
    root_logger.info("")
    root_logger.info(f"Now importing '{imported_function}' function from '{module_name}' module...")
    root_logger.info("")
    get_run_logger().info("")
    get_run_logger().info(f"Now importing '{imported_function}' function from '{module_name}' module...")
    get_run_logger().info("")



@task
def load_data_to_raw_flight_destinations_tbl():
    from dwh_pipelines.L1_raw_layer.raw_flight_destinations_tbl         import  load_flight_destinations_data_to_raw_table
    module_name = 'dwh_pipelines.L1_raw_layer.raw_flight_destinations_tbl'
    imported_function = 'load_flight_destinations_data_to_raw_table'
    root_logger.info("")
    root_logger.info(f"Now importing '{imported_function}' function from '{module_name}' module...")
    root_logger.info("")
    get_run_logger().info("")
    get_run_logger().info(f"Now importing '{imported_function}' function from '{module_name}' module...")
    get_run_logger().info("")



@task
def load_data_to_raw_flight_promotion_deals_tbl():
    from dwh_pipelines.L1_raw_layer.raw_flight_promotion_deals_tbl      import  load_flight_promotion_deals_data_to_raw_table
    module_name = 'dwh_pipelines.L1_raw_layer.raw_flight_promotion_deals_tbl'
    imported_function = 'load_flight_promotion_deals_data_to_raw_table'
    root_logger.info("")
    root_logger.info(f"Now importing '{imported_function}' function from '{module_name}' module...")
    root_logger.info("")
    get_run_logger().info("")
    get_run_logger().info(f"Now importing '{imported_function}' function from '{module_name}' module...")
    get_run_logger().info("")



@task
def load_data_to_raw_flight_schedules_tbl():
    from dwh_pipelines.L1_raw_layer.raw_flight_schedules_tbl            import  load_flight_schedules_data_to_raw_table
    module_name = 'dwh_pipelines.L1_raw_layer.raw_flight_schedules_tbl'
    imported_function = 'load_flight_schedules_data_to_raw_table'
    root_logger.info("")
    root_logger.info(f"Now importing '{imported_function}' function from '{module_name}' module...")
    root_logger.info("")
    get_run_logger().info("")
    get_run_logger().info(f"Now importing '{imported_function}' function from '{module_name}' module...")
    get_run_logger().info("")



@task
def load_data_to_raw_flight_ticket_sales_tbl():
    from dwh_pipelines.L1_raw_layer.raw_flight_ticket_sales_tbl         import  load_flight_ticket_sales_data_to_raw_table
    module_name = 'dwh_pipelines.L1_raw_layer.raw_flight_ticket_sales_tbl'
    imported_function = 'load_flight_ticket_sales_data_to_raw_table'
    root_logger.info("")
    root_logger.info(f"Now importing '{imported_function}' function from '{module_name}' module...")
    root_logger.info("")
    get_run_logger().info("")
    get_run_logger().info(f"Now importing '{imported_function}' function from '{module_name}' module...")
    get_run_logger().info("")



@task
def load_data_to_raw_sales_agents_tbl():
    from dwh_pipelines.L1_raw_layer.raw_sales_agents_tbl                import  load_sales_agents_data_to_raw_table
    module_name = 'dwh_pipelines.L1_raw_layer.raw_sales_agents_tbl'
    imported_function = 'load_sales_agents_data_to_raw_table'
    root_logger.info("")
    root_logger.info(f"Now importing '{imported_function}' function from '{module_name}' module...")
    root_logger.info("")
    get_run_logger().info("")
    get_run_logger().info(f"Now importing '{imported_function}' function from '{module_name}' module...")
    get_run_logger().info("")



@task
def load_data_to_raw_ticket_prices_tbl():
    from dwh_pipelines.L1_raw_layer.raw_ticket_prices_tbl               import  load_ticket_prices_data_to_raw_table
    module_name = 'dwh_pipelines.L1_raw_layer.raw_ticket_prices_tbl'
    imported_function = 'load_ticket_prices_data_to_raw_table'
    root_logger.info("")
    root_logger.info(f"Now importing '{imported_function}' function from '{module_name}' module...")
    root_logger.info("")
    get_run_logger().info("")
    get_run_logger().info(f"Now importing '{imported_function}' function from '{module_name}' module...")
    get_run_logger().info("")








# # ============================================== 2. STAGING LAYER ============================================== 

# Set up tasks for staging layer

@task
def load_data_to_stg_accommodation_bookings_table():
    from dwh_pipelines.L2_staging_layer.dev.stg_accommodation_bookings_tbl      import  load_data_to_stg_accommodation_bookings_table
    module_name = 'dwh_pipelines.L2_stg_layer.stg_accommodation_bookings_tbl'
    imported_function = 'load_accommodation_bookings_data_to_stg_table'
    root_logger.info("")
    root_logger.info(f"Now importing '{imported_function}' function from '{module_name}' module...")
    root_logger.info("")
    get_run_logger().info("")
    get_run_logger().info(f"Now importing '{imported_function}' function from '{module_name}' module...")
    get_run_logger().info("")




@task
def load_data_to_stg_customer_feedbacks_table():
    from dwh_pipelines.L2_staging_layer.dev.stg_customer_feedbacks_tbl      import  load_data_to_stg_customer_feedbacks_table
    module_name = 'dwh_pipelines.L2_stg_layer.stg_customer_feedbacks_tbl'
    imported_function = 'load_customer_feedbacks_data_to_stg_table'
    root_logger.info("")
    root_logger.info(f"Now importing '{imported_function}' function from '{module_name}' module...")
    root_logger.info("")
    get_run_logger().info("")
    get_run_logger().info(f"Now importing '{imported_function}' function from '{module_name}' module...")
    get_run_logger().info("")



@task
def load_data_to_stg_customer_info_table():
    from dwh_pipelines.L2_staging_layer.dev.stg_customer_info_tbl      import  load_data_to_stg_customer_info_table
    module_name = 'dwh_pipelines.L2_stg_layer.stg_customer_info_tbl'
    imported_function = 'load_customer_info_data_to_stg_table'
    root_logger.info("")
    root_logger.info(f"Now importing '{imported_function}' function from '{module_name}' module...")
    root_logger.info("")
    get_run_logger().info("")
    get_run_logger().info(f"Now importing '{imported_function}' function from '{module_name}' module...")
    get_run_logger().info("")




@task
def load_data_to_stg_flight_bookings_table():
    from dwh_pipelines.L2_staging_layer.dev.stg_flight_bookings_tbl      import  load_data_to_stg_flight_bookings_table
    module_name = 'dwh_pipelines.L2_stg_layer.stg_flight_bookings_tbl'
    imported_function = 'load_flight_bookings_data_to_stg_table'
    root_logger.info("")
    root_logger.info(f"Now importing '{imported_function}' function from '{module_name}' module...")
    root_logger.info("")
    get_run_logger().info("")
    get_run_logger().info(f"Now importing '{imported_function}' function from '{module_name}' module...")
    get_run_logger().info("")



@task
def load_data_to_stg_flight_destinations_table():
    from dwh_pipelines.L2_staging_layer.dev.stg_flight_destinations_tbl      import  load_data_to_stg_flight_destinations_table
    module_name = 'dwh_pipelines.L2_stg_layer.stg_flight_destinations_tbl'
    imported_function = 'load_flight_destinations_data_to_stg_table'
    root_logger.info("")
    root_logger.info(f"Now importing '{imported_function}' function from '{module_name}' module...")
    root_logger.info("")
    get_run_logger().info("")
    get_run_logger().info(f"Now importing '{imported_function}' function from '{module_name}' module...")
    get_run_logger().info("")




@task
def load_data_to_stg_flight_promotion_deals_table():
    from dwh_pipelines.L2_staging_layer.dev.stg_flight_promotion_deals_tbl      import  load_data_to_stg_flight_promotion_deals_table
    module_name = 'dwh_pipelines.L2_stg_layer.stg_flight_promotion_deals_tbl'
    imported_function = 'load_flight_promotion_deals_data_to_stg_table'
    root_logger.info("")
    root_logger.info(f"Now importing '{imported_function}' function from '{module_name}' module...")
    root_logger.info("")
    get_run_logger().info("")
    get_run_logger().info(f"Now importing '{imported_function}' function from '{module_name}' module...")
    get_run_logger().info("")




@task
def load_data_to_stg_flight_schedules_table():
    from dwh_pipelines.L2_staging_layer.dev.stg_flight_schedules_tbl      import  load_data_to_stg_flight_schedules_table
    module_name = 'dwh_pipelines.L2_stg_layer.stg_flight_schedules_tbl'
    imported_function = 'load_flight_schedules_data_to_stg_table'
    root_logger.info("")
    root_logger.info(f"Now importing '{imported_function}' function from '{module_name}' module...")
    root_logger.info("")
    get_run_logger().info("")
    get_run_logger().info(f"Now importing '{imported_function}' function from '{module_name}' module...")
    get_run_logger().info("")




@task
def load_data_to_stg_flight_ticket_sales_table():
    from dwh_pipelines.L2_staging_layer.dev.stg_flight_ticket_sales_tbl      import  load_data_to_stg_flight_ticket_sales_table
    module_name = 'dwh_pipelines.L2_stg_layer.stg_flight_ticket_sales_tbl'
    imported_function = 'load_flight_ticket_sales_data_to_stg_table'
    root_logger.info("")
    root_logger.info(f"Now importing '{imported_function}' function from '{module_name}' module...")
    root_logger.info("")
    get_run_logger().info("")
    get_run_logger().info(f"Now importing '{imported_function}' function from '{module_name}' module...")
    get_run_logger().info("")



@task
def load_data_to_stg_sales_agents_table():
    from dwh_pipelines.L2_staging_layer.dev.stg_sales_agents_tbl      import  load_data_to_stg_sales_agents_table
    module_name = 'dwh_pipelines.L2_stg_layer.stg_sales_agents_tbl'
    imported_function = 'load_sales_agents_data_to_stg_table'
    root_logger.info("")
    root_logger.info(f"Now importing '{imported_function}' function from '{module_name}' module...")
    root_logger.info("")
    get_run_logger().info("")
    get_run_logger().info(f"Now importing '{imported_function}' function from '{module_name}' module...")
    get_run_logger().info("")



@task
def load_data_to_stg_ticket_prices_table():
    from dwh_pipelines.L2_staging_layer.dev.stg_ticket_prices_tbl      import  load_data_to_stg_ticket_prices_table
    module_name = 'dwh_pipelines.L2_stg_layer.stg_ticket_prices_tbl'
    imported_function = 'load_ticket_prices_data_to_stg_table'
    root_logger.info("")
    root_logger.info(f"Now importing '{imported_function}' function from '{module_name}' module...")
    root_logger.info("")
    get_run_logger().info("")
    get_run_logger().info(f"Now importing '{imported_function}' function from '{module_name}' module...")
    get_run_logger().info("")







# Set up tasks for running DQ tests in staging layer


@task
def run_dq_test_for_stg_accommodation_bookings_tbl():
    test_name = "test_stg_accommodation_bookings_tbl.py"
    test_filepath =  os.path.abspath('dwh_pipelines/L2_staging_layer/tests/test_stg_accommodation_bookings_tbl.py')
    result = subprocess.run(['pytest', test_filepath], capture_output=True ) 
    output = result.stdout.decode('utf-8')
    root_logger.info("")
    root_logger.info(f"Now running the '{test_name}' script...")
    root_logger.info("")
    get_run_logger().info("")
    get_run_logger().info(f"Now running the '{test_name}' script...")
    get_run_logger().info("")

    if "FAILED" in output:
        raise ValueError(f"One or more tests in '{test_name}' test has failed... ")
    else:
        root_logger.info("")
        root_logger.info(f"SUCCESS - All tests for '{test_name}' test have passed!")
        root_logger.info("")
        get_run_logger().info("")
        get_run_logger().info(f"SUCCESS - All tests for '{test_name}' test have passed!")
        get_run_logger().info("")



@task
def run_dq_test_for_stg_customer_feedbacks_tbl():
    test_name = "test_stg_customer_feedbacks_tbl.py"
    test_filepath =  os.path.abspath('dwh_pipelines/L2_staging_layer/tests/test_stg_customer_feedbacks_tbl.py')
    result = subprocess.run(['pytest', test_filepath], capture_output=True ) 
    output = result.stdout.decode('utf-8')
    root_logger.info("")
    root_logger.info(f"Now running the '{test_name}' script...")
    root_logger.info("")
    get_run_logger().info("")
    get_run_logger().info(f"Now running the '{test_name}' script...")
    get_run_logger().info("")

    if "FAILED" in output:
        raise ValueError(f"One or more tests in '{test_name}' test has failed... ")
    else:
        root_logger.info("")
        root_logger.info(f"SUCCESS - All tests for '{test_name}' test have passed!")
        root_logger.info("")
        get_run_logger().info("")
        get_run_logger().info(f"SUCCESS - All tests for '{test_name}' test have passed!")
        get_run_logger().info("")

@task
def run_dq_test_for_stg_customer_info_tbl():
    test_name = "test_stg_customer_info_tbl.py"
    test_filepath =  os.path.abspath('dwh_pipelines/L2_staging_layer/tests/test_stg_customer_info_tbl.py')
    result = subprocess.run(['pytest', test_filepath], capture_output=True ) 
    output = result.stdout.decode('utf-8')
    root_logger.info("")
    root_logger.info(f"Now running the '{test_name}' script...")
    root_logger.info("")
    get_run_logger().info("")
    get_run_logger().info(f"Now running the '{test_name}' script...")
    get_run_logger().info("")

    if "FAILED" in output:
        raise ValueError(f"One or more tests in '{test_name}' test has failed... ")
    else:
        root_logger.info("")
        root_logger.info(f"SUCCESS - All tests for '{test_name}' test have passed!")
        root_logger.info("")
        get_run_logger().info("")
        get_run_logger().info(f"SUCCESS - All tests for '{test_name}' test have passed!")
        get_run_logger().info("")


@task
def run_dq_test_for_stg_flight_bookings_tbl():
    test_name = "test_stg_flight_bookings_tbl.py"
    test_filepath =  os.path.abspath('dwh_pipelines/L2_staging_layer/tests/test_stg_flight_bookings_tbl.py')
    result = subprocess.run(['pytest', test_filepath], capture_output=True ) 
    output = result.stdout.decode('utf-8')
    root_logger.info("")
    root_logger.info(f"Now running the '{test_name}' script...")
    root_logger.info("")
    get_run_logger().info("")
    get_run_logger().info(f"Now running the '{test_name}' script...")
    get_run_logger().info("")

    if "FAILED" in output:
        raise ValueError(f"One or more tests in '{test_name}' test has failed... ")
    else:
        root_logger.info("")
        root_logger.info(f"SUCCESS - All tests for '{test_name}' test have passed!")
        root_logger.info("")
        get_run_logger().info("")
        get_run_logger().info(f"SUCCESS - All tests for '{test_name}' test have passed!")
        get_run_logger().info("")


@task
def run_dq_test_for_stg_flight_destinations_tbl():
    test_name = "test_stg_flight_destinations_tbl.py"
    test_filepath =  os.path.abspath('dwh_pipelines/L2_staging_layer/tests/test_stg_flight_destinations_tbl.py')
    result = subprocess.run(['pytest', test_filepath], capture_output=True ) 
    output = result.stdout.decode('utf-8')
    root_logger.info("")
    root_logger.info(f"Now running the '{test_name}' script...")
    root_logger.info("")
    get_run_logger().info("")
    get_run_logger().info(f"Now running the '{test_name}' script...")
    get_run_logger().info("")

    if "FAILED" in output:
        raise ValueError(f"One or more tests in '{test_name}' test has failed... ")
    else:
        root_logger.info("")
        root_logger.info(f"SUCCESS - All tests for '{test_name}' test have passed!")
        root_logger.info("")
        get_run_logger().info("")
        get_run_logger().info(f"SUCCESS - All tests for '{test_name}' test have passed!")
        get_run_logger().info("")


@task
def run_dq_test_for_stg_flight_promotion_deals_tbl():
    test_name = "test_stg_flight_promotion_deals_tbl.py"
    test_filepath =  os.path.abspath('dwh_pipelines/L2_staging_layer/tests/test_stg_flight_promotion_deals_tbl.py')
    result = subprocess.run(['pytest', test_filepath], capture_output=True ) 
    output = result.stdout.decode('utf-8')
    root_logger.info("")
    root_logger.info(f"Now running the '{test_name}' script...")
    root_logger.info("")
    get_run_logger().info("")
    get_run_logger().info(f"Now running the '{test_name}' script...")
    get_run_logger().info("")

    if "FAILED" in output:
        raise ValueError(f"One or more tests in '{test_name}' test has failed... ")
    else:
        root_logger.info("")
        root_logger.info(f"SUCCESS - All tests for '{test_name}' test have passed!")
        root_logger.info("")
        get_run_logger().info("")
        get_run_logger().info(f"SUCCESS - All tests for '{test_name}' test have passed!")
        get_run_logger().info("")


@task
def run_dq_test_for_stg_flight_schedules_tbl():
    test_name = "test_stg_flight_schedules_tbl.py"
    test_filepath =  os.path.abspath('dwh_pipelines/L2_staging_layer/tests/test_stg_flight_schedules_tbl.py')
    result = subprocess.run(['pytest', test_filepath], capture_output=True ) 
    output = result.stdout.decode('utf-8')
    root_logger.info("")
    root_logger.info(f"Now running the '{test_name}' script...")
    root_logger.info("")
    get_run_logger().info("")
    get_run_logger().info(f"Now running the '{test_name}' script...")
    get_run_logger().info("")

    if "FAILED" in output:
        raise ValueError(f"One or more tests in '{test_name}' test has failed... ")
    else:
        root_logger.info("")
        root_logger.info(f"SUCCESS - All tests for '{test_name}' test have passed!")
        root_logger.info("")
        get_run_logger().info("")
        get_run_logger().info(f"SUCCESS - All tests for '{test_name}' test have passed!")
        get_run_logger().info("")


@task
def run_dq_test_for_stg_flight_ticket_sales_tbl():
    test_name = "test_stg_flight_ticket_sales_tbl.py"
    test_filepath =  os.path.abspath('dwh_pipelines/L2_staging_layer/tests/test_stg_flight_ticket_sales_tbl.py')
    result = subprocess.run(['pytest', test_filepath], capture_output=True ) 
    output = result.stdout.decode('utf-8')
    root_logger.info("")
    root_logger.info(f"Now running the '{test_name}' script...")
    root_logger.info("")
    get_run_logger().info("")
    get_run_logger().info(f"Now running the '{test_name}' script...")
    get_run_logger().info("")

    if "FAILED" in output:
        raise ValueError(f"One or more tests in '{test_name}' test has failed... ")
    else:
        root_logger.info("")
        root_logger.info(f"SUCCESS - All tests for '{test_name}' test have passed!")
        root_logger.info("")
        get_run_logger().info("")
        get_run_logger().info(f"SUCCESS - All tests for '{test_name}' test have passed!")
        get_run_logger().info("")


@task
def run_dq_test_for_stg_sales_agents_tbl():
    test_name = "test_stg_sales_agents_tbl.py"
    test_filepath =  os.path.abspath('dwh_pipelines/L2_staging_layer/tests/test_stg_sales_agents_tbl.py')
    result = subprocess.run(['pytest', test_filepath], capture_output=True ) 
    output = result.stdout.decode('utf-8')
    root_logger.info("")
    root_logger.info(f"Now running the '{test_name}' script...")
    root_logger.info("")
    get_run_logger().info("")
    get_run_logger().info(f"Now running the '{test_name}' script...")
    get_run_logger().info("")

    if "FAILED" in output:
        raise ValueError(f"One or more tests in '{test_name}' test has failed... ")
    else:
        root_logger.info("")
        root_logger.info(f"SUCCESS - All tests for '{test_name}' test have passed!")
        root_logger.info("")
        get_run_logger().info("")
        get_run_logger().info(f"SUCCESS - All tests for '{test_name}' test have passed!")
        get_run_logger().info("")


@task
def run_dq_test_for_stg_ticket_prices_tbl():
    test_name = "test_stg_ticket_prices_tbl.py"
    test_filepath =  os.path.abspath('dwh_pipelines/L2_staging_layer/tests/test_stg_ticket_prices_tbl.py')
    result = subprocess.run(['pytest', test_filepath], capture_output=True ) 
    output = result.stdout.decode('utf-8')
    root_logger.info("")
    root_logger.info(f"Now running the '{test_name}' script...")
    root_logger.info("")
    get_run_logger().info("")
    get_run_logger().info(f"Now running the '{test_name}' script...")
    get_run_logger().info("")

    if "FAILED" in output:
        raise ValueError(f"One or more tests in '{test_name}' test has failed... ")
    else:
        root_logger.info("")
        root_logger.info(f"SUCCESS - All tests for '{test_name}' test have passed!")
        root_logger.info("")
        get_run_logger().info("")
        get_run_logger().info(f"SUCCESS - All tests for '{test_name}' test have passed!")
        get_run_logger().info("")





@task
def create_prod_environment_for_staging_layer():
    from dwh_pipelines.L2_staging_layer.prod.create_stg_prod_env      import  create_prod_environment_for_staging
    module_name = 'dwh_pipelines.L2_staging_layer.prod.create_prod_env'
    imported_function = 'create_prod_environment_for_staging'
    root_logger.info("")
    root_logger.info(f"Now importing '{imported_function}' function from '{module_name}' module to create STAGING PROD environment ...")
    root_logger.info("")
    get_run_logger().info("")
    get_run_logger().info(f"Now importing '{imported_function}' function from '{module_name}' module to create STAGING PROD environment ...")
    get_run_logger().info("")
    






# ============================================== FLOWS ==============================================
# ====================================================================================================



# ============================================== 0. DATA GENERATION ==============================================


# Set up sub-flow for generating travel data 
@flow(name="Generate travel data", flow_run_name="generate_travel_data_flow")
def run_data_generation_flow():
    return generate_synthetic_travel_data()







# # ============================================== 1. RAW LAYER ============================================== 


# Set up sub-flow for executing tasks in raw layer 
@flow(name="Execute tasks in raw layer", flow_run_name="source_to_raw_layer_flow")
def run_raw_layer_flow():

    load_data_to_raw_accommodation_bookings_tbl()
    root_logger.info("SUCCESS! Completed loading source data into 'raw_accommodation_bookings_tbl'! ")
    get_run_logger().info("SUCCESS! Completed loading source data into 'raw_accommodation_bookings_tbl'! ")



    load_data_to_raw_customer_feedbacks_tbl()
    root_logger.info("SUCCESS! Completed loading source data into 'raw_customer_feedbacks_tbl'! ")
    get_run_logger().info("SUCCESS! Completed loading source data into 'raw_customer_feedbacks_tbl'! ")



    load_data_to_raw_customer_info_tbl()
    root_logger.info("SUCCESS! Completed loading source data into 'raw_customer_info_tbl'! ")
    get_run_logger().info("SUCCESS! Completed loading source data into 'raw_customer_info_tbl'! ")



    load_data_to_raw_flight_bookings_tbl()
    root_logger.info("SUCCESS! Completed loading source data into 'raw_flight_bookings_tbl'! ")
    get_run_logger().info("SUCCESS! Completed loading source data into 'raw_flight_bookings_tbl'! ")



    load_data_to_raw_flight_destinations_tbl()
    root_logger.info("SUCCESS! Completed loading source data into 'raw_flight_destinations_tbl'! ")
    get_run_logger().info("SUCCESS! Completed loading source data into 'raw_flight_destinations_tbl'! ")



    load_data_to_raw_flight_promotion_deals_tbl()
    root_logger.info("SUCCESS! Completed loading source data into 'raw_flight_promotion_deals_tbl'! ")
    get_run_logger().info("SUCCESS! Completed loading source data into 'raw_flight_promotion_deals_tbl'! ")



    load_data_to_raw_flight_schedules_tbl()
    root_logger.info("SUCCESS! Completed loading source data into 'raw_flight_schedules_tbl'! ")
    get_run_logger().info("SUCCESS! Completed loading source data into 'raw_flight_schedules_tbl'! ")



    load_data_to_raw_flight_ticket_sales_tbl()
    root_logger.info("SUCCESS! Completed loading source data into 'raw_flight_ticket_sales_tbl'! ")
    get_run_logger().info("SUCCESS! Completed loading source data into 'raw_flight_ticket_sales_tbl'! ")



    load_data_to_raw_sales_agents_tbl()
    root_logger.info("SUCCESS! Completed loading source data into 'raw_sales_agents_tbl'! ")
    get_run_logger().info("SUCCESS! Completed loading source data into 'raw_sales_agents_tbl'! ")



    load_data_to_raw_ticket_prices_tbl()
    root_logger.info("SUCCESS! Completed loading source data into 'raw_ticket_prices_tbl'! ")
    get_run_logger().info("SUCCESS! Completed loading source data into 'raw_ticket_prices_tbl'! ")


    root_logger.info("Now terminating session for raw layer tasks...")
    root_logger.info("")
    root_logger.info("Raw tables processing session ended.")
    get_run_logger().info("Now terminating session for raw layer tasks...")
    get_run_logger().info("")
    get_run_logger().info("Raw tables processing session ended.")






# # ============================================== 2. STAGING LAYER ============================================== 



# Set up sub-flow for executing tasks in staging layer 
@flow(name="Execute tasks in staging layer", flow_run_name="raw_to_stg_layer_flow")
def run_staging_layer_flow():

    load_data_to_stg_accommodation_bookings_table()
    root_logger.info("SUCCESS! Completed loading raw data into 'stg_accommodation_bookings_table'! ")
    get_run_logger().info("SUCCESS! Completed loading raw data into 'stg_accommodation_bookings_table'! ")



    load_data_to_stg_customer_feedbacks_table()
    root_logger.info("SUCCESS! Completed loading raw data into 'stg_customer_feedbacks_table'! ")
    get_run_logger().info("SUCCESS! Completed loading raw data into 'stg_customer_feedbacks_table'! ")



    load_data_to_stg_customer_info_table()
    root_logger.info("SUCCESS! Completed loading raw data into 'stg_customer_info_table'! ")
    get_run_logger().info("SUCCESS! Completed loading raw data into 'stg_customer_info_table'! ")



    load_data_to_stg_flight_bookings_table()
    root_logger.info("SUCCESS! Completed loading raw data into 'stg_flight_bookings_table'! ")
    get_run_logger().info("SUCCESS! Completed loading raw data into 'stg_flight_bookings_table'! ")



    load_data_to_stg_flight_destinations_table()
    root_logger.info("SUCCESS! Completed loading raw data into 'stg_flight_destinations_table'! ")
    get_run_logger().info("SUCCESS! Completed loading raw data into 'stg_flight_destinations_table'! ")



    load_data_to_stg_flight_promotion_deals_table()
    root_logger.info("SUCCESS! Completed loading raw data into 'stg_flight_promotion_deals_table'! ")
    get_run_logger().info("SUCCESS! Completed loading raw data into 'stg_flight_promotion_deals_table'! ")



    load_data_to_stg_flight_schedules_table()
    root_logger.info("SUCCESS! Completed loading raw data into 'stg_flight_schedules_table'! ")
    get_run_logger().info("SUCCESS! Completed loading raw data into 'stg_flight_schedules_table'! ")



    load_data_to_stg_flight_ticket_sales_table()
    root_logger.info("SUCCESS! Completed loading raw data into 'stg_flight_ticket_sales_table'! ")
    get_run_logger().info("SUCCESS! Completed loading raw data into 'stg_flight_ticket_sales_table'! ")



    load_data_to_stg_sales_agents_table()
    root_logger.info("SUCCESS! Completed loading raw data into 'stg_sales_agents_table'! ")
    get_run_logger().info("SUCCESS! Completed loading raw data into 'stg_sales_agents_table'! ")



    load_data_to_stg_ticket_prices_table()
    root_logger.info("SUCCESS! Completed loading raw data into 'stg_ticket_prices_table'! ")
    get_run_logger().info("SUCCESS! Completed loading raw data into 'stg_ticket_prices_table'! ")


    root_logger.info("Now terminating session for staging layer tasks...")
    root_logger.info("")
    root_logger.info("Staging tables processing session ended.")
    get_run_logger().info("Now terminating session for staging layer tasks...")
    get_run_logger().info("")
    get_run_logger().info("Staging tables processing session ended.")




# Set up sub-flow for executing tasks in staging layer 
@flow(name="Run DQ test for staging tables", flow_run_name="dq_tests_for_stg_layer_flow")
def run_dq_tests_for_staging_layer_flow():

    # Set up flow for running DQ tests in staging layer
    run_dq_test_for_stg_accommodation_bookings_tbl()
    root_logger.info("SUCCESS! Completed performing DQ tests on the 'stg_accommodation_bookings_tbl' table! ")
    get_run_logger().info("SUCCESS! Completed performing DQ tests on the 'stg_accommodation_bookings_tbl' table! ")



    run_dq_test_for_stg_customer_feedbacks_tbl()
    root_logger.info("SUCCESS! Completed performing DQ tests on the 'stg_customer_feedbacks_tbl' table! ")
    get_run_logger().info("SUCCESS! Completed performing DQ tests on the 'stg_customer_feedbacks_tbl' table! ")



    run_dq_test_for_stg_customer_info_tbl()
    root_logger.info("SUCCESS! Completed performing DQ tests on the 'stg_customer_info_tbl' table! ")
    get_run_logger().info("SUCCESS! Completed performing DQ tests on the 'stg_customer_info_tbl' table! ")


    run_dq_test_for_stg_flight_bookings_tbl()
    root_logger.info("SUCCESS! Completed performing DQ tests on the 'stg_customer_info_tbl' table! ")
    get_run_logger().info("SUCCESS! Completed performing DQ tests on the 'stg_customer_info_tbl' table! ")



    run_dq_test_for_stg_flight_destinations_tbl()
    root_logger.info("SUCCESS! Completed performing DQ tests on the 'stg_flight_destinations_tb' table! ")
    get_run_logger().info("SUCCESS! Completed performing DQ tests on the 'stg_flight_destinations_tb' table! ")



    run_dq_test_for_stg_flight_promotion_deals_tbl()
    root_logger.info("SUCCESS! Completed performing DQ tests on the 'stg_customer_info_tbl' table! ")
    get_run_logger().info("SUCCESS! Completed performing DQ tests on the 'stg_customer_info_tbl' table! ")



    run_dq_test_for_stg_flight_schedules_tbl()
    root_logger.info("SUCCESS! Completed performing DQ tests on the 'flight_promotion_deals_tbl' table! ")
    get_run_logger().info("SUCCESS! Completed performing DQ tests on the 'flight_promotion_deals_tbl' table! ")



    run_dq_test_for_stg_flight_ticket_sales_tbl()
    root_logger.info("SUCCESS! Completed performing DQ tests on the 'flight_ticket_sales_tbl' table! ")
    get_run_logger().info("SUCCESS! Completed performing DQ tests on the 'flight_ticket_sales_tbl' table! ")



    run_dq_test_for_stg_sales_agents_tbl()
    root_logger.info("SUCCESS! Completed performing DQ tests on the 'stg_sales_agents_tbl' table! ")
    get_run_logger().info("SUCCESS! Completed performing DQ tests on the 'stg_sales_agents_tbl' table! ")



    run_dq_test_for_stg_ticket_prices_tbl()
    root_logger.info("SUCCESS! Completed performing DQ tests on the 'stg_ticket_prices_tbl' table! ")
    get_run_logger().info("SUCCESS! Completed performing DQ tests on the 'stg_ticket_prices_tbl' table! ")



# Set up sub-flow for creating PROD environment for staging layer 
@flow(name="Create PROD environment for staging layer", flow_run_name="creating_staging_prod_env_flow")
def run_stg_prod_env_creation_flow():

    create_prod_environment_for_staging_layer()
    root_logger.info("SUCCESS! Completed creating a PRODUCTION environment for STAGING layer. ")
    get_run_logger().info("SUCCESS! Completed creating a PRODUCTION environment for STAGING layer. ")






# ============================================== FLOW RUNS ==============================================
# ====================================================================================================


# Specify flow execution order in DAG-less manner  
if __name__=="__main__":
    

    # L0 - Generate data
    run_data_generation_flow()


    # L1 - Extract source data into raw tables 
    run_raw_layer_flow()
    

    # L2 - Extract raw data into staging tables
    run_staging_layer_flow()
    run_dq_tests_for_staging_layer_flow()
    
    run_stg_prod_env_creation_flow()