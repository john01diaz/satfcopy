code_process_name = \
    'silver_device_catalogue'

# code_process_name = \
#     'gold_device_catalogue'

input_tables = \
    {
        'table1': 'test',
        'table2': 'test'
    }

globals()[code_process_name](input_tables=input_tables)
