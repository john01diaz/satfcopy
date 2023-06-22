import os
import shutil
from nf_common_source.code.services.file_system_service.objects.folders import Folders
from nf_common_source.code.services.reporting_service.reporters.log_with_datetime import log_message
from nf_common_source.code.services.reporting_service.wrappers.run_and_log_function_wrapper import run_and_log_function

from sat_etl_script_source.b_code.common.etl_files_converter_stage_01_output_folder_tree_creator import \
    create_etl_files_converter_stage_01_output_folder_tree
from sat_etl_script_source.b_code.common.helpers.new_folder_creator import create_new_folder


@run_and_log_function
def orchestrate_shell_etl_files_converter_stage_01(
        input_root_folder: Folders,
        stage_name: str) \
        -> None:
    log_message(
        'separate sql and python files into separate folders')

    log_message(
        message='input filepath:' + input_root_folder.absolute_path_string)

    relative_path_removal = \
        input_root_folder.parent_absolute_path_string + os.sep

    sql_extension_folder, python_extension_folder = \
        create_etl_files_converter_stage_01_output_folder_tree(
            stage_name=stage_name)

    __process_input_root_folder_children(
        input_root_folder=input_root_folder,
        relative_path_removal=relative_path_removal,
        sql_extension_folder=sql_extension_folder,
        python_extension_folder=python_extension_folder)


def __process_input_root_folder_children(
        input_root_folder: Folders,
        relative_path_removal: str,
        sql_extension_folder: Folders,
        python_extension_folder: Folders) \
        -> None:
    for input_folder_path, dirs, filenames \
            in os.walk(input_root_folder.absolute_path_string):
        relative_path = \
            input_folder_path.replace(relative_path_removal, '')

        log_message(
            message='processing folder:  ' + relative_path)

        output_sql_extension_folder_path = \
            os.path.join(
                sql_extension_folder.absolute_path_string,
                relative_path)

        output_python_extension_folder_path = \
            os.path.join(
                python_extension_folder.absolute_path_string,
                relative_path)

        create_new_folder(
            folder_path=output_sql_extension_folder_path)

        create_new_folder(
            folder_path=output_python_extension_folder_path)

        __process_children_files(
            input_folder_path=input_folder_path,
            output_sql_extension_folder_path=output_sql_extension_folder_path,
            output_python_extension_folder_path=output_python_extension_folder_path,
            filenames=filenames)


def __process_children_files(
        input_folder_path: str,
        output_sql_extension_folder_path: str,
        output_python_extension_folder_path: str,
        filenames: list):
    for filename \
            in filenames:
        __process_child_file(
            filename=filename,
            input_folder_path=input_folder_path,
            output_sql_extension_folder_path=output_sql_extension_folder_path,
            output_python_extension_folder_path=output_python_extension_folder_path)


def __process_child_file(
        filename: str,
        input_folder_path: str,
        output_sql_extension_folder_path: str,
        output_python_extension_folder_path: str) \
        -> None:
    log_message(
        message='processing file:      ' + filename)

    input_file_path = \
        os.path.join(
            input_folder_path,
            filename)

    if filename[-4:] == '.sql':
        output_file_path = \
            os.path.join(
                output_sql_extension_folder_path,
                filename)

        shutil.copyfile(
            input_file_path,
            output_file_path)

    elif filename[-3:] == '.py':
        output_file_path = \
            os.path.join(
                output_python_extension_folder_path,
                filename)

        shutil.copyfile(
            input_file_path,
            output_file_path)

    else:
        log_message(
            message='Skipping unrecognized file extension: ' + filename)
