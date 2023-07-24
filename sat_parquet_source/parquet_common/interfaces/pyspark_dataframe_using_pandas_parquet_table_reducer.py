from nf_common_source.code.services.file_system_service.objects.folders import Folders
from sat_parquet_source.parquet_common.pyspark_session_getter import get_pyspark_session
from sat_parquet_source.parquet_common.table_converters.folder_to_delta_table_converter import \
    convert_folder_to_delta_table
from sat_parquet_source.parquet_common.table_writers.pyspark_dataframe_as_parquet_table_using_pandas_exporter import \
    export_pyspark_dataframe_as_parquet_table_using_pandas
from sat_parquet_source.parquet_tables_reducer.b_code.helpers.reduced_parquet_table_getter import \
    get_reduced_parquet_table


def reduce_pyspark_dataframe_using_pandas_parquet_table(
        number_of_rows_to_keep: int,
        parquet_folder_path: str,
        output_root_folder: Folders) \
        -> None:
    spark_session = \
        get_pyspark_session()

    reduced_parquet_table_as_pyspark_dataframe = \
        get_reduced_parquet_table(
            spark_session=spark_session,
            number_of_rows_to_keep=number_of_rows_to_keep,
            parquet_folder_path=parquet_folder_path)

    export_pyspark_dataframe_as_parquet_table_using_pandas(
        output_root_folder=output_root_folder,
        pyspark_dataframe=reduced_parquet_table_as_pyspark_dataframe)

    convert_folder_to_delta_table(
        output_root_folder=output_root_folder,
        spark_session=spark_session)

    spark_session.stop()
