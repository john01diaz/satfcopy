

def get_filtered_bie_ids(
        database_records_dictionary: dict,
        desired_key: str,
        table_name: str) \
        -> list:
    bie_table_ids_to_run = \
        list()

    process_table_configurations = \
        database_records_dictionary['etl_run'][table_name]

    for process_table_configuration \
            in process_table_configurations:
        bie_table_ids_to_run.append(
            process_table_configuration[desired_key])

    return \
        bie_table_ids_to_run
