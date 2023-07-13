import pyspark.sql
from nf_common_source.code.services.file_system_service.objects.folders import Folders


def export_pyspark_dataframe_as_parquet_table_using_pyspark(
        output_root_folder: Folders,
        pyspark_dataframe: pyspark.sql.DataFrame) \
        -> None:
    pyspark_dataframe.write.save(
        path=output_root_folder.absolute_path_string,
        format="delta",
        mode='overwrite',
        overwriteSchema=True)
