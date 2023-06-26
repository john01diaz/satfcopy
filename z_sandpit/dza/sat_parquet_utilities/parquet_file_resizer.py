import glob
from nf_common_source.code.services.datetime_service.time_helpers.time_getter import now_time_as_string_for_files
from nf_common_source.code.services.file_system_service.objects.folders import Folders
from sat_parquet_source.b_code.helpers.spark_session_creator import create_pyspark_session


def __resize_parquet_file(
        parquet_file_paths: list):
    spark_session = \
        create_pyspark_session(
            session_name_string='parquet_file_resizer' + now_time_as_string_for_files())

    for parquet_file_path \
            in parquet_file_paths:
        pyspark_dataframe = \
            spark_session \
                .read \
                .option("mergeSchema", "true") \
                .parquet(parquet_file_path) \
                .limit(1000)

        pyspark_dataframe.write.mode("overwrite").parquet(
            parquet_file_path)

    spark_session.stop()


if __name__ == '__main__':
    child_parquet_folder = \
        Folders(
            absolute_path_string='/Users/terraire/bWa/DZa/etl/collect/blob/blob-temp-anusha_folder-sigraph_bronze_2023_05_21_1500/sigraph_bronze/CS_Layer_Loop_Loop_elements_latest')

    input_root_folder_child_path_children = \
        glob.glob(
            child_parquet_folder.absolute_path_string + "/**/*.snappy.parquet",
            recursive=True)

    __resize_parquet_file(
        parquet_file_paths=input_root_folder_child_path_children)

    exit(0)
