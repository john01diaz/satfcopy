import os.path
import glob
from deltalake import DeltaTable
from nf_common_source.code.services.datetime_service.time_helpers.time_getter import now_time_as_string_for_files
from sat_parquet_source.parquet_schema_analyzer.b_code import create_pyspark_session


def load_and_register_table(
        file_configuration: list) \
        -> None:
    match file_configuration[1]:
        case 'parquet':
            __clean_old_parquet_files_in_parquet_folder(
                file_configuration=file_configuration)

        case other:
            pass


def __clean_old_parquet_files_in_parquet_folder(
        file_configuration: list) \
        -> None:
    user_specific_absolute_path_to_relative_path = \
        file_configuration[0]

    relative_path = \
        file_configuration[2]

    file_name_folder = \
        file_configuration[3]

    print(
        'Running on: ' + str(relative_path) + ' - ' + str(file_name_folder))

    absolute_file_name_folder_path = \
        os.path.join(
            user_specific_absolute_path_to_relative_path,
            relative_path,
            file_name_folder)

    input_root_folder_children = \
        glob.glob(
            absolute_file_name_folder_path + "/*.parquet",
            recursive=True)

    if not input_root_folder_children:
        print(
            'The are not parquet files.')

        return

    delta_table = \
        DeltaTable(
            absolute_file_name_folder_path)

    spark_session = \
        create_pyspark_session(
            session_name_string='parquet_file_resizer' + now_time_as_string_for_files())

    pyspark_dataframe = \
        spark_session \
            .read \
            .option("mergeSchema", "true") \
            .parquet(absolute_file_name_folder_path) \
            .limit(1000)

    __keep_latest_parquet_files_only(
        input_root_folder_children=input_root_folder_children,
        delta_table=delta_table)

    print(
        'Folder processed: ' + str(os.sep.join(absolute_file_name_folder_path.split(os.sep)[7:]))
        + ' Shape: ')


def __keep_latest_parquet_files_only(
        input_root_folder_children: list,
        delta_table) \
        -> None:
    # TODO: this is the code to keep the latest parquet files only - commented it out to get the process working
    for input_root_folder_child \
            in input_root_folder_children:
        latest_parquet_file_paths = \
            delta_table.file_uris()

        input_root_folder_child_normalized = \
            input_root_folder_child.replace('\\', '/')

        if latest_parquet_file_paths:
            if input_root_folder_child_normalized not in latest_parquet_file_paths:
                os.remove(
                    input_root_folder_child)

                print(
                    'File removed: ' + str(input_root_folder_child))
