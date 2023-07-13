from typing import Optional
from nf_common_source.code.services.reporting_service.reporters.log_with_datetime import log_message
from pandas import DataFrame
from sat_common_source.table_readers.table_loaders.aql_table_getter import \
    get_aql_table
from sat_common_source.table_readers.table_loaders.csv_table_getter import \
    get_csv_table
from sat_common_source.table_readers.table_loaders.xlsx_table_getter import \
    get_xlsx_table
from sat_workflow_source.b_code.etl_processes_wrapper.objects.table_configurations import TableConfigurations


def get_simple_table(
        etl_processes_wrapper_registry,
        table_configuration: TableConfigurations) \
        -> Optional[DataFrame]:
    match table_configuration.source_extension_type:
        case 'xlsx':
            table = \
                get_xlsx_table(
                    etl_processes_wrapper_registry=etl_processes_wrapper_registry,
                    table_configuration=table_configuration)

        case 'csv':
            table = \
                get_csv_table(
                    etl_processes_wrapper_registry=etl_processes_wrapper_registry,
                    table_configuration=table_configuration)

        case 'aql':
            table = \
                get_aql_table(
                    etl_processes_wrapper_registry=etl_processes_wrapper_registry,
                    table_configuration=table_configuration)

        case _:
            log_message(
                'WARNING - table not loaded (no source info) - ' + table_configuration.table_name)

            return

    return \
        table
