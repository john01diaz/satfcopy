import os.path
import glob
from deltalake import DeltaTable


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

    # TODO: code to delete the old parquet files only.
    #  It cannot load largest parquet files like CS_Layer_Loop_Loop_elements
    __delete_old_parquet_files(
        input_root_folder_children=input_root_folder_children,
        delta_table=delta_table)

    table = \
        delta_table.to_pyarrow_table().to_pandas()

    print(
        'Folder processed: ' + str(os.sep.join(absolute_file_name_folder_path.split(os.sep)[7:]))
        + ' Shape: ' + str(table.shape))


def __delete_old_parquet_files(
        input_root_folder_children: list,
        delta_table) \
        -> None:
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
