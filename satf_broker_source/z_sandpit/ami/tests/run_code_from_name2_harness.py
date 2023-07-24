from satf_broker_source.z_sandpit.ami.tests import run_code_process

# code_process_name = \
#     'silver_device_catalogue'

code_process_name = \
    'gold_device_catalogue'

input_tables = \
    {
        'table1': 'test',
        'table2': 'test'
    }

run_code_process(
    code_process_name=code_process_name,
    input_tables=input_tables)
