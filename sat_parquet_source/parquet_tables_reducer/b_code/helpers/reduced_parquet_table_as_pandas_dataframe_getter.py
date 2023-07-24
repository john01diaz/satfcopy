import pandas
from sat_parquet_source.parquet_common.table_getters.parquet_table_using_pandas_getter import \
    get_parquet_table_using_delta_lake


def get_reduced_parquet_table_as_pandas_dataframe(
        number_of_rows_to_keep: int,
        parquet_folder_path: str) \
        -> pandas.DataFrame:
    pandas_dataframe = \
        get_parquet_table_using_delta_lake(
            absolute_table_name_folder_path=parquet_folder_path)

    reduced_pandas_dataframe = \
        pandas_dataframe.head(
            number_of_rows_to_keep)

    return \
        reduced_pandas_dataframe
