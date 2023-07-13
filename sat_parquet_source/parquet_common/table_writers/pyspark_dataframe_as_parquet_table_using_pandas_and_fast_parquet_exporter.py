import os.path
import pyspark.sql
import fastparquet

from nf_common_source.code.services.file_system_service.objects.folders import Folders


def export_pyspark_dataframe_as_parquet_table_using_pandas_and_fast_parquet(
        output_root_folder: Folders,
        pyspark_dataframe: pyspark.sql.DataFrame) \
        -> None:
    pandas_dataframe = \
        pyspark_dataframe.toPandas()

    if not os.path.exists(output_root_folder.absolute_path_string):
        os.makedirs(
            output_root_folder.absolute_path_string)

    fastparquet.write(
        'pandas_dataframe_to_fast_parquet.parquet',
        pandas_dataframe,
        compression='GZIP')
