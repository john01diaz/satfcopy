import os.path
import pandas
from nf_common_source.code.services.file_system_service.objects.files import Files
from nf_common_source.code.services.input_output_service.delimited_text.utf_8_csv_reader import \
    convert_utf_8_csv_with_header_file_to_dataframe

from sat_workflow_source.b_code.etl_processes_wrapper.objects.table_configurations import TableConfigurations


def get_csv_table(
        etl_processes_wrapper_registry,
        table_configuration: TableConfigurations) \
        -> pandas.DataFrame:
    etl_root_folder_path \
        = etl_processes_wrapper_registry.owning_etl_processes_wrapper_universe.etl_processes_wrapper_configuration.etl_root_folder_path

    absolute_path_string = \
        os.path.join(
            etl_root_folder_path,
            'collect',
            table_configuration.table_source_relative_path,
            table_configuration.table_source_folder,
            table_configuration.table_name + '.csv')

    table = \
        convert_utf_8_csv_with_header_file_to_dataframe(
            Files(
                absolute_path_string=absolute_path_string))

    return \
        table
