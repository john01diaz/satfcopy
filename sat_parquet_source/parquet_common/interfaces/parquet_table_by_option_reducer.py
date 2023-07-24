from sat_parquet_source.parquet_common.interfaces.pandas_dataframe_using_fastparquet_reducer import \
    reduce_pandas_dataframe_using_fastparquet
from sat_parquet_source.parquet_common.interfaces.pandas_dataframe_using_pyarrow_reducer import \
    reduce_pandas_dataframe_using_pyarrow
from sat_parquet_source.parquet_common.interfaces.pyspark_dataframe_using_pyarrow_reducer import \
    reduce_pyspark_dataframe_using_pyarrow
from sat_parquet_source.parquet_common.interfaces.pyspark_dataframe_using_pandas_parquet_table_reducer import \
    reduce_pyspark_dataframe_using_pandas_parquet_table
from sat_parquet_source.parquet_common.interfaces.pyspark_dataframe_using_fastparquet_reducer import \
    reduce_pyspark_dataframe_using_fastparquet
from sat_parquet_source.parquet_common.interfaces.pyspark_dataframe_using_pyspark_reducer import \
    reduce_pyspark_dataframe_using_pyspark


def reduce_parquet_table_by_option(
        reduce_parquet_table_configuration,
        number_of_rows_to_keep,
        parquet_folder_path,
        output_parquet_table_path) \
        -> None:
    match reduce_parquet_table_configuration.value:
        case reduce_parquet_table_configuration.OPTION_PYSPARK_PANDAS.value:
            reduce_pyspark_dataframe_using_pandas_parquet_table(
                number_of_rows_to_keep=number_of_rows_to_keep,
                parquet_folder_path=parquet_folder_path,
                output_root_folder=output_parquet_table_path)

        case reduce_parquet_table_configuration.OPTION_PYSPARK_PYSPARK.value:
            reduce_pyspark_dataframe_using_pyspark(
                number_of_rows_to_keep=number_of_rows_to_keep,
                parquet_folder_path=parquet_folder_path,
                output_root_folder=output_parquet_table_path)

        case reduce_parquet_table_configuration.OPTION_PYSPARK_FASTPARQUET.value:
            reduce_pyspark_dataframe_using_fastparquet(
                number_of_rows_to_keep=number_of_rows_to_keep,
                parquet_folder_path=parquet_folder_path,
                output_root_folder=output_parquet_table_path)

        case reduce_parquet_table_configuration.OPTION_PYSPARK_PYARROW.value:
            reduce_pyspark_dataframe_using_pyarrow(
                number_of_rows_to_keep=number_of_rows_to_keep,
                parquet_folder_path=parquet_folder_path,
                output_root_folder=output_parquet_table_path)

        case reduce_parquet_table_configuration.OPTION_PANDAS_PYARROW.value:
            reduce_pandas_dataframe_using_pyarrow(
                number_of_rows_to_keep=number_of_rows_to_keep,
                parquet_folder_path=parquet_folder_path,
                output_root_folder=output_parquet_table_path)

        case reduce_parquet_table_configuration.OPTION_PANDAS_FASTPARQUET.value:
            reduce_pandas_dataframe_using_fastparquet(
                number_of_rows_to_keep=number_of_rows_to_keep,
                parquet_folder_path=parquet_folder_path,
                output_root_folder=output_parquet_table_path)

        case _:
            raise \
                NotImplementedError
