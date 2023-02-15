@task
def run_dq_test_for_stg_xxxxxxxxxxxxxxxs_tbl():
    test_name = "test_stg_xxxxxxxxxxxxxxxs_tbl.py"
    test_filepath =  os.path.abspath('dwh_pipelines/L2_staging_layer/tests/test_stg_xxxxxxxxxxxxxxxs_tbl.py')
    result = subprocess.run(['pytest', test_filepath], capture_output=True ) 
    output = result.stdout.decode('utf-8')
    if "FAILED" in output:
        raise ValueError(f"One or more tests in '{test_name}' test has failed... ")
    else:
        root_logger.info("SUCCESS - All tests have passed!")