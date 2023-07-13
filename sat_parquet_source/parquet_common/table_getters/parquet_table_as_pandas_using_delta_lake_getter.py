import pandas
from sat_parquet_source.parquet_common.table_getters.parquet_table_as_delta_table_using_delta_lake_getter import \
    get_parquet_table_as_delta_table_using_delta_lake


def get_parquet_table_as_pandas_using_delta_lake(
        absolute_table_name_folder_path: str) \
        -> pandas.DataFrame:
    delta_table = \
        get_parquet_table_as_delta_table_using_delta_lake(
            absolute_table_name_folder_path)

    table = \
        delta_table.to_pyarrow_table().to_pandas()

    return \
        table
