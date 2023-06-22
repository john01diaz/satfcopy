import json


def export_dictionary_to_json_file(
        etl_json_configuration_file_path: str,
        database_records_dictionary: dict) \
        -> None:
    with open(etl_json_configuration_file_path, 'w') as file_writer:
        json.dump(
            database_records_dictionary,
            file_writer,
            indent=4)
