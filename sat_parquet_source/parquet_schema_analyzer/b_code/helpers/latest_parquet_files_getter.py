import json
import os
import glob
from nf_common_source.code.services.file_system_service.objects.folders import Folders
from nf_common_source.code.services.reporting_service.reporters.log_with_datetime import log_message


def get_latest_parquet_files(
        child_parquet_folder: Folders,
        input_root_folder_child_path_children: list) \
        -> list:
    removing_instructions = \
        __get_removing_instructions_json_file(
            child_parquet_folder=child_parquet_folder)

    __get_list_of_latest_parquet_files(
        removing_instructions=removing_instructions,
        input_root_folder_child_path_children=input_root_folder_child_path_children)

    return \
        input_root_folder_child_path_children


def __get_removing_instructions_json_file(
        child_parquet_folder: Folders) \
        -> list:
    removing_instructions = \
        list()

    json_file_lines = \
        list()

    json_settings_paths = \
        glob.glob(
            child_parquet_folder.absolute_path_string + os.sep + '_delta_log' + os.sep + "/**/*.json",
            recursive=True)

    for latest_json_settings_path \
            in json_settings_paths:
        with open(latest_json_settings_path) as file:
            json_file_lines = \
                file.readlines()

            file.close()

        for json_file_line \
                in json_file_lines:
            latest_json_settings_content = \
                json.loads(json_file_line)

            if list(latest_json_settings_content.keys())[0] == 'remove':
                removing_instructions.append(
                    latest_json_settings_content)

    return \
        removing_instructions


def __get_list_of_latest_parquet_files(
        removing_instructions: list,
        input_root_folder_child_path_children: list) \
        -> None:
    for removing_instruction \
            in removing_instructions:
        removed_parquet_file_name = \
            removing_instruction['remove']['path']

        matching_parquet_filenames = \
            [parquet_file_path for parquet_file_path
             in input_root_folder_child_path_children
             if parquet_file_path.endswith(removed_parquet_file_name)]

        if not matching_parquet_filenames:
            log_message(
                message='No matches were found.')

        for matching_parquet_filename \
                in matching_parquet_filenames:
            if matching_parquet_filename in input_root_folder_child_path_children:
                input_root_folder_child_path_children.remove(
                    matching_parquet_filename)

            else:
                log_message(
                    message='Expecting to find file in the list but it didn\'t find: ' + matching_parquet_filename)
