import os


def populate_file_configuration_list(
        stage_name: str,
        input_root_folder_children: list,
        input_parquet_table_path: str,
        file_configuration_list: list) \
        -> list:
    for stage_folder_child_stage_folder_path_child \
            in input_root_folder_children:
        __add_file_configuration(
            file_configuration_list=file_configuration_list,
            stage_folder_child_stage_folder_path_child=stage_folder_child_stage_folder_path_child,
            stage_name=stage_name,
            input_parquet_table_path=input_parquet_table_path)

    return \
        file_configuration_list


def __add_file_configuration(
        file_configuration_list: list,
        stage_folder_child_stage_folder_path_child: str,
        stage_name: str,
        input_parquet_table_path: str) \
        -> None:
    if os.path.isdir(stage_folder_child_stage_folder_path_child):
        file_configuration = \
            [
                input_parquet_table_path,
                'snappy.parquet',
                stage_name,
                stage_folder_child_stage_folder_path_child.split(os.sep)[-1],
                'input',
                None,
                ''
            ]

        file_configuration_list.append(
            file_configuration)
