import glob
import os
import shutil
from nf_common_source.code.services.datetime_service.time_helpers.time_getter import now_time_as_string_for_files
from nf_common_source.code.services.file_system_service.objects.folders import Folders
from sat_parquet_source.parquet_schema_analyzer.b_code.helpers.spark_session_creator import create_pyspark_session


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
                .limit(100)

        pyspark_dataframe.write.mode("overwrite").parquet(
            parquet_file_path)

        created_parquet_files = \
            glob.glob(
                parquet_file_path + "/*.snappy.parquet",
                recursive=False)

        if len(created_parquet_files) > 1:
            raise Exception

        created_parquet_file = \
            created_parquet_files[0]

        original_file_name = \
            parquet_file_path.split(os.sep)[-1]

        created_parquet_filename = \
            created_parquet_file.split(os.sep)[-1]

        reduced_file_path = \
            os.sep.join(parquet_file_path.split(os.sep)[:-1]) + os.sep + created_parquet_filename

        shutil.copy2(
            created_parquet_file,
            reduced_file_path)

        new_name_for_reduced_file_path = \
            os.sep.join(reduced_file_path.split(os.sep)[:-1]) + os.sep + original_file_name

        shutil.rmtree(
            os.sep.join(created_parquet_file.split(os.sep)[:-1]))

        os.rename(
            reduced_file_path,
            new_name_for_reduced_file_path)

        print(
            'Processed file: ' + str(original_file_name))

    spark_session.stop()


if __name__ == '__main__':
    child_parquet_folder = \
        Folders(
            absolute_path_string=r'/Users/terraire/bWa/DZa/etl/collect/blob_latest/clean_parquet_2023_07_11_09_20_22_CS_Layer_Loop_Loop_elements/blob-temp-anusha_folder-sigraph_bronze_2023_06_27_1817/CS_Layer_Loop_Loop_elements')

    input_root_folder_child_path_children = \
        glob.glob(
            child_parquet_folder.absolute_path_string + "/**/*.snappy.parquet",
            recursive=True)

    __resize_parquet_file(
        parquet_file_paths=input_root_folder_child_path_children)

    exit(0)
