from typing import Optional
from pandas import DataFrame
from sat_common_source.table_readers.table_loaders.simple_table_getter import get_simple_table
from sat_workflow_source.b_code.etl_processes_wrapper.helpers.data_policy_to_source_table_applier import \
    migrate_source_table_to_bclearer_data_policy
from sat_parquet_source.parquet_common.table_getters.parquet_table_getter import \
    get_parquet_table
from sat_workflow_source.b_code.etl_processes_wrapper.objects.table_configurations import TableConfigurations


def get_table(
        etl_processes_wrapper_registry,
        table_configuration: TableConfigurations) \
        -> Optional[DataFrame]:
    if table_configuration.source_extension_type == 'snappy.parquet':
        table = \
            get_parquet_table(
                etl_processes_wrapper_registry=etl_processes_wrapper_registry,
                table_configuration=table_configuration)

    else:
        table = \
            get_simple_table(
                etl_processes_wrapper_registry=etl_processes_wrapper_registry,
                table_configuration=table_configuration)

    if table is None:
        return

    cleaned_table = \
        migrate_source_table_to_bclearer_data_policy(
            source_table=table)

    return \
        cleaned_table
