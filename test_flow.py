import os 
import subprocess
from prefect import task, Flow

@task
def run_data_generator_script():
    subprocess.run(["python", "dwh_pipelines/0_src_data_generator/src_data_generator.py"])

@task
def run_raw_layer_scripts():
    for filename in os.listdir("dwh_pipelines/1_raw_layer"):
        if filename.endswith(".py"):
            subprocess.run(["python", f"dwh_pipelines/1_raw_layer/{filename}"])


with Flow("Execute raw layer scripts") as flow:
    generate_data_task = run_data_generator_script()
    execute_raw_layer_tasks = run_raw_layer_scripts()


    generate_data_task.set_upstream(execute_raw_layer_tasks)

flow.run()