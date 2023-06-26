import pandas
from nf_common_source.code.services.file_system_service.objects.folders import Folders
from nf_common_source.code.services.reporting_service.reporters.log_with_datetime import log_message
from pyspark.sql import SparkSession
# from pyspark.sql import functions
# from pyspark.sql.types import StringType
import pyarrow.parquet as pyarrow_parquet


def read_parquet_file(
        spark_session: SparkSession,
        working_output_folder: Folders,
        input_file_system_object_path: str,
        data_chunk_size: int) \
        -> tuple:
    log_message(
        message='*' * 25 + 'Starting to read the parquet element.')

    parquet_file = \
        pyarrow_parquet.ParquetFile(
            input_file_system_object_path)

    row_count = \
        parquet_file.metadata.num_rows

    if data_chunk_size:
        pyspark_dataframe = \
            spark_session \
                .read \
                .option("mergeSchema", "true") \
                .parquet(input_file_system_object_path) \
                .limit(data_chunk_size)

        log_message(
            message='*' * 25 + 'Parquet element reading is DONE. Number of rows: ' + str(row_count))

    else:
        pyspark_dataframe = \
            spark_session \
                .read \
                .option("mergeSchema", "true") \
                .parquet(input_file_system_object_path)

        log_message(
            message='*' * 25 + 'Parquet element reading is DONE. Number of rows: ' + str(row_count))

    # log_message(
    #     message='*' * 25 + 'Exporting parquet file content to csv.')
    #
    # pyspark_dataframe = \
    #     pyspark_dataframe.select(
    #         [functions.col(c).cast(StringType()).alias(c) for c in pyspark_dataframe.columns])
    #
    # pyspark_dataframe.write \
    #     .options(header='True', delimiter=',')\
    #     .csv(working_output_folder.absolute_path_string)
    #
    # log_message(
    #     message='*' * 25 + 'Exporting parquet file content is DONE.')

    log_message(
        message='*' * 25 + 'Converting PySparkDataframe to pandas.')

    pyspark_dataframe_to_pandas = \
        pyspark_dataframe.toPandas().copy()

    dtypes_tuples = \
        pyspark_dataframe.schema.fields

    del pyspark_dataframe

    log_message(
        message='*' * 25 + 'Converting PySparkDataframe to pandas is DONE.')

    return \
        pyspark_dataframe_to_pandas, \
        row_count, \
        dtypes_tuples
