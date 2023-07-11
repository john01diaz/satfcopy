import os.path
import pandas

from sat_workflow_source.b_code.etl_processes_wrapper.helpers.aveva_loader_data_policy_applier import \
    migrate_table_from_bclearer_to_aveva_data_policy
from sat_workflow_source.b_code.etl_processes_wrapper.objects.table_configurations import TableConfigurations


def get_xlsx_table(
        etl_processes_wrapper_registry,
        table_configuration: TableConfigurations) \
        -> pandas.DataFrame:
    etl_root_folder_path \
        = etl_processes_wrapper_registry.owning_etl_processes_wrapper_universe.etl_processes_wrapper_configuration.etl_root_folder_path

    absolute_file_path = \
        os.path.join(
            etl_root_folder_path,
            'collect',
            table_configuration.table_source_relative_path,
            table_configuration.table_source_folder,
            table_configuration.table_name + '.xlsx')

    table = \
        pandas.read_excel(
            absolute_file_path,
            sheet_name=table_configuration.loader_excel_sheet_name,
            dtype=str,
            na_filter=False)

    cleaned_table = \
        migrate_table_from_bclearer_to_aveva_data_policy(
            table=table)

    return \
        cleaned_table
