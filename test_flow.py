import os 
import psycopg2
from prefect import task, Flow
from sqlalchemy import create_engine
from configparser import ConfigParser
from dwh_pipelines.L0_src_data_generator.src_data_generator import generate_travel_data


# Add environment variables for setting up Postgres connection 
config = ConfigParser()

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



# Set up tasks
 
@task(log_prints=False)
def run_data_generator_script():
    generate_travel_data()



@task
def run_raw_layer_scripts(postgres_connection):
    for filename in os.listdir("dwh_pipelines/L1_raw_layer"):
        if filename.endswith(".py"):
            module_name = filename.split(".")[0]
            module = __import__(f"dwh_pipelines.L1_raw_layer.{module_name}")
            load_data_func = getattr(module, "load_data_to_raw_table")
            print(f'Function: {load_data_func} ')
            load_data_func()


# Set up main workflow (DAG)
with Flow("Execute raw layer scripts") as flow:
    generate_data_task = run_data_generator_script()
    execute_raw_layer_tasks = run_raw_layer_scripts(postgres_connection)

    
    execute_raw_layer_tasks.set_upstream(generate_data_task)

flow.run(logging_config={"handlers": {"console": {"level": "ERROR"}}})

