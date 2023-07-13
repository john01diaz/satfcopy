from enum import Enum
import pandas


def get_column_filtered_parquet_table_as_pandas_using_pyarrow_and_enum(
        absolute_table_name_folder_path: str,
        filter_enum: Enum) \
        -> pandas.DataFrame:
    columns = \
        [column.value for column in filter_enum]

    table = \
        pandas.read_parquet(
            absolute_table_name_folder_path,
            engine='pyarrow',
            columns=columns)

    return \
        table
