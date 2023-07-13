import pandas
from sat_common_source.table_readers.helpers.absolute_path_for_table_getter import get_absolute_path_for_table
from sat_parquet_source.parquet_common.table_getters.column_filtered_parquet_table_as_pandas_using_pyarrow_and_enum_name_getter import \
    get_column_filtered_parquet_as_pandas_table_using_pyarrow_and_enum_name
from sat_parquet_source.parquet_common.table_getters.parquet_table_as_pandas_using_delta_lake_getter import \
    get_parquet_table_as_pandas_using_delta_lake
from sat_workflow_source.b_code.etl_processes.common.global_flags import GlobalFlags
from sat_workflow_source.b_code.etl_processes_wrapper.objects.table_configurations import TableConfigurations


def get_parquet_table(
        etl_processes_wrapper_registry,
        table_configuration: TableConfigurations) \
        -> pandas.DataFrame:
    absolute_table_name_folder_path = \
        get_absolute_path_for_table(
            etl_processes_wrapper_registry=etl_processes_wrapper_registry,
            table_configuration=table_configuration)

    if table_configuration.process_table_columns_filter:
        table = \
            get_column_filtered_parquet_as_pandas_table_using_pyarrow_and_enum_name(
                absolute_table_name_folder_path=absolute_table_name_folder_path,
                filter_enum_name=table_configuration.process_table_columns_filter)

        return \
            table

    table = \
        get_parquet_table_as_pandas_using_delta_lake(
            absolute_table_name_folder_path)

    if GlobalFlags.FILTER_TO_DATABASE_LIST:
        table = \
            table[table['database_name'] == 'R_2016R3']

    return \
        table
