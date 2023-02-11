import os
import psycopg2
from prefect import task, Flow
from dwh_pipelines.L0_src_data_generator.src_data_generator import generate_travel_data
from dwh_pipelines.L1_raw_layer.raw_accommodation_bookings_tbl import load_data_to_raw_table




@task
def generate_travel_data():
    generate_travel_data()

@task
def load_data_to_raw_table():
    load_data_to_raw_table()

with Flow("Execute raw layer scripts") as flow:
    try:
        generate_data_task = generate_travel_data()
        execute_raw_layer_tasks = load_data_to_raw_table()

        execute_raw_layer_tasks.set_upstream(generate_data_task)
    except Exception as e:
        print(e)
        
flow.run()
