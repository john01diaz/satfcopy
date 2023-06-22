

def get_json_configuration_as_dictionary() \
        -> dict:
    json_configurations_as_dictionary = \
        {
            'etl_run': {
                'table_configurations': [],
                'processes': [],
                'process_table_configuration': []
            }
        }

    return \
        json_configurations_as_dictionary
