from prefect import task, Flow
from dwh_pipelines.L0_src_data_generator.src_data_generator import generate_travel_data
from dwh_pipelines.L1_raw_layer.raw_accommodation_bookings_tbl import load_accommodation_bookings_data_to_raw_table



# Set up source data generation task
@task
def generate_synthetic_travel_data():
    generate_travel_data()


# Set up raw layer tasks 
@task
def load_data_to_raw_accommodation_bookings_tbl():
    load_accommodation_bookings_data_to_raw_table()






with Flow("Execute raw layer scripts") as flow:
    try:
        generate_source_data = generate_synthetic_travel_data()

        load_src_data_to_raw_accommodation_bookings_tbl = load_data_to_raw_accommodation_bookings_tbl()
        
        load_src_data_to_raw_accommodation_bookings_tbl.set_upstream(generate_source_data)

    except Exception as e:
        print(e)
        
flow.run()
