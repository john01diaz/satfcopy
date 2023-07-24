from nf_common_source.code.services.file_system_service.objects.folders import Folders
from sat_parquet_source.parquet_common.table_converters.folder_to_delta_table_converter import \
    convert_folder_to_delta_table
from sat_parquet_source.parquet_common.table_writers.pandas_dataframe_as_parquet_table_using_fastparquet_exporter import \
    export_pandas_dataframe_as_parquet_table_using_fastparquet
from sat_parquet_source.parquet_tables_reducer.b_code.helpers.reduced_parquet_table_as_pandas_dataframe_getter import \
    get_reduced_parquet_table_as_pandas_dataframe


def reduce_pandas_dataframe_using_fastparquet(
        number_of_rows_to_keep: int,
        parquet_folder_path: str,
        output_root_folder: Folders) \
        -> None:
    reduced_parquet_table_as_pandas_dataframe = \
        get_reduced_parquet_table_as_pandas_dataframe(
            number_of_rows_to_keep=number_of_rows_to_keep,
            parquet_folder_path=parquet_folder_path)

    export_pandas_dataframe_as_parquet_table_using_fastparquet(
        output_root_folder=output_root_folder,
        pandas_dataframe=reduced_parquet_table_as_pandas_dataframe)

    convert_folder_to_delta_table(
        output_root_folder=output_root_folder)
