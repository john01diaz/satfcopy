import os
import pyspark
from nf_common_source.code.services.file_system_service.objects.folders import Folders
from nf_common_source.code.services.identification_services.hash_service.hash_creator import create_identity_hash_string


def export_pyspark_dataframe_as_parquet_table_using_pyarrow(
        output_root_folder: Folders,
        pyspark_dataframe: pyspark.sql.DataFrame) \
        -> None:
    uuid = \
        create_identity_hash_string(
                [pyspark_dataframe])

    if not os.path.exists(output_root_folder.absolute_path_string):
        os.makedirs(
            output_root_folder.absolute_path_string)

    pandas_dataframe = \
        pyspark_dataframe.toPandas()

    pandas_dataframe.to_parquet(
        path=output_root_folder.absolute_path_string + os.sep + str(uuid) + '.snappy.parquet',
        engine='pyarrow',
        compression='snappy')
