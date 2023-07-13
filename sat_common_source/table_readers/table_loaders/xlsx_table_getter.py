import pandas

from sat_common_source.table_readers.helpers.absolute_path_for_table_getter import get_absolute_path_for_table
from sat_workflow_source.b_code.etl_processes_wrapper.helpers.aveva_loader_data_policy_applier import \
    migrate_table_from_bclearer_to_aveva_data_policy
from sat_workflow_source.b_code.etl_processes_wrapper.objects.table_configurations import TableConfigurations


def get_xlsx_table(
        etl_processes_wrapper_registry,
        table_configuration: TableConfigurations) \
        -> pandas.DataFrame:
    absolute_path_for_table = \
        get_absolute_path_for_table(
            etl_processes_wrapper_registry=etl_processes_wrapper_registry,
            table_configuration=table_configuration)

    table = \
        pandas.read_excel(
            absolute_path_for_table,
            sheet_name=table_configuration.loader_excel_sheet_name,
            dtype=str,
            na_filter=False)

    cleaned_table = \
        migrate_table_from_bclearer_to_aveva_data_policy(
            table=table)

    return \
        cleaned_table
