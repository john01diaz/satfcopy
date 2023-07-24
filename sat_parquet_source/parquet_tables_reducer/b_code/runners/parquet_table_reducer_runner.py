from delta.tables import DeltaTable
from nf_common_source.code.services.file_system_service.objects.folders import Folders
from nf_common_source.code.services.reporting_service.reporters.log_with_datetime import log_message
from sat_parquet_source.parquet_common.helpers.parquet_folder_path_getter import get_parquet_folder_path
from sat_parquet_source.parquet_common.pyspark_session_getter import get_pyspark_session
from sat_parquet_source.parquet_common.table_writers.pyspark_dataframe_as_parquet_table_using_pandas_exporter import \
    export_pyspark_dataframe_as_parquet_table_using_pandas
from sat_parquet_source.parquet_tables_reducer.b_code.helpers.reduced_parquet_table_getter import \
    get_reduced_parquet_table


def run_parquet_table_reducer(
        number_of_rows_to_keep: int,
        file_configuration: list,
        output_root_folder: Folders) \
        -> None:
    if file_configuration[1] != 'snappy.parquet':
        return

    __parquet_table_reducer_hard_crash_wrapper(
        number_of_rows_to_keep=number_of_rows_to_keep,
        file_configuration=file_configuration,
        output_root_folder=output_root_folder)


def __parquet_table_reducer_hard_crash_wrapper(
        number_of_rows_to_keep: int,
        file_configuration: list,
        output_root_folder: Folders):
    try:
        parquet_folder_path = \
            get_parquet_folder_path(
                file_configuration=file_configuration)

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

        DeltaTable.convertToDelta(
            spark_session,
            "parquet.`{0}`".format(
                output_root_folder.absolute_path_string))

        if not DeltaTable.isDeltaTable(spark_session, output_root_folder.absolute_path_string):
            raise Exception

        spark_session.stop()

    except Exception as error:
        log_message(
            message='*'*30 + ' ERROR: ' + file_configuration[3] + ' - ' + str(error))
