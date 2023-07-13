import os.path
import pyspark.sql
from nf_common_source.code.services.file_system_service.objects.folders import Folders


def export_pyspark_dataframe_as_parquet_table_using_pandas(
        output_root_folder: Folders,
        pyspark_dataframe: pyspark.sql.DataFrame) \
        -> None:
    pandas_dataframe = \
        pyspark_dataframe.toPandas()

    if not os.path.exists(output_root_folder.absolute_path_string):
        os.makedirs(
            output_root_folder.absolute_path_string)

    pandas_dataframe.to_parquet(
        path=output_root_folder.absolute_path_string + os.sep + 'file.snappy.parquet',
        engine='auto',
        compression='snappy')

    # pandas_dataframe.to_parquet(
    #     path=output_root_folder.absolute_path_string + os.sep + 'file.snappy.parquet',
    #     engine='pyarrow',
    #     compression='snappy')

    # pandas_dataframe.to_parquet(
    #     path=output_root_folder.absolute_path_string + os.sep + 'file.snappy.parquet',
    #     engine='fastparquet',
    #     compression='snappy')
