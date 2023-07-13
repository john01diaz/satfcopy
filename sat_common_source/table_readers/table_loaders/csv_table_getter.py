import pandas
from nf_common_source.code.services.file_system_service.objects.files import Files
from nf_common_source.code.services.input_output_service.delimited_text.utf_8_csv_reader import \
    convert_utf_8_csv_with_header_file_to_dataframe

from sat_common_source.table_readers.helpers.absolute_path_for_table_getter import get_absolute_path_for_table
from sat_workflow_source.b_code.etl_processes_wrapper.objects.table_configurations import TableConfigurations


def get_csv_table(
        etl_processes_wrapper_registry,
        table_configuration: TableConfigurations) \
        -> pandas.DataFrame:
    absolute_path_for_table = \
        get_absolute_path_for_table(
            etl_processes_wrapper_registry=etl_processes_wrapper_registry,
            table_configuration=table_configuration)

    table = \
        convert_utf_8_csv_with_header_file_to_dataframe(
            Files(
                absolute_path_string=absolute_path_for_table))

    return \
        table
