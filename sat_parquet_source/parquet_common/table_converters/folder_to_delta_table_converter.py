import pyspark.sql
from delta.tables import DeltaTable
from nf_common_source.code.services.file_system_service.objects.folders import Folders
from sat_parquet_source.parquet_common.pyspark_session_getter import get_pyspark_session


def convert_folder_to_delta_table(
        output_root_folder: Folders,
        spark_session: pyspark.sql.SparkSession = None) \
        -> None:
    if not spark_session:
        spark_session = \
            get_pyspark_session()

    DeltaTable.convertToDelta(
        spark_session,
        "parquet.`{0}`".format(
            output_root_folder.absolute_path_string))

    if not DeltaTable.isDeltaTable(spark_session, output_root_folder.absolute_path_string):
        raise Exception

    spark_session.stop()
