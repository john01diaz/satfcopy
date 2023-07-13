import pandas
from sat_parquet_source.parquet_common.table_getters.column_filtered_parquet_table_as_pandas_using_pyarrow_and_enum_getter import \
    get_column_filtered_parquet_table_as_pandas_using_pyarrow_and_enum
from sat_workflow_source.b_code.etl_processes_wrapper.objects.helpers.enum_from_string.enum_getter import get_enum


def get_column_filtered_parquet_as_pandas_table_using_pyarrow_and_enum_name(
        absolute_table_name_folder_path: str,
        filter_enum_name: str) \
        -> pandas.DataFrame:
    filter_enum = \
        get_enum(
            enum_name=filter_enum_name)

    table = \
        get_column_filtered_parquet_table_as_pandas_using_pyarrow_and_enum(
            absolute_table_name_folder_path=absolute_table_name_folder_path,
            filter_enum=filter_enum)

    return \
        table
