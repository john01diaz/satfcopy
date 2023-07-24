import os
from nf_common_source.code.services.file_system_service.objects.folders import Folders
from nf_common_source.code.services.reporting_service.reporters.log_file import LogFiles
from nf_common_source.code.services.reporting_service.reporters.log_with_datetime import log_message
from nf_common_source.code.services.reporting_service.wrappers.run_and_log_function_wrapper import run_and_log_function

from sat_etl_script_source.b_code.common.helpers.new_folder_creator import create_new_folder
from sat_etl_script_source.b_code.common.helpers.string_replacers.qualify_string_to_having_replacer import \
    replace_qualify_string_to_having


@run_and_log_function
def orchestrate_shell_etl_files_converter_stage_06(
        input_root_folder: Folders,
        previous_stage_name: str,
        stage_name: str) \
        -> None:
    log_message(
        message='input filepath:' + input_root_folder.absolute_path_string)

    sql_extension_folder_path = \
        os.path.join(
            LogFiles.folder_path,
            previous_stage_name)

    current_stage_folder_path = \
        os.path.join(
            LogFiles.folder_path,
            stage_name)

    create_new_folder(
        folder_path=current_stage_folder_path)

    for input_folder_path, dirs, filenames \
            in os.walk(sql_extension_folder_path):
        current_stage_input_folder_path = \
            input_folder_path.replace(
                previous_stage_name,
                stage_name)

        create_new_folder(
            folder_path=current_stage_input_folder_path)

        if not filenames:
            continue

        for filename \
                in filenames:
            if filename[-4:] == '.sql':
                __process_file(
                    filename=filename,
                    previous_stage_name=previous_stage_name,
                    stage_name=stage_name,
                    input_folder_path=input_folder_path)


def __process_file(
        filename: str,
        previous_stage_name: str,
        stage_name: str,
        input_folder_path: str) \
        -> None:
    log_message(
        message='processing file:      ' + filename)

    input_file_path = \
        os.path.join(
            input_folder_path,
            filename)

    __process_sql_file(
        previous_stage_name=previous_stage_name,
        stage_name=stage_name,
        input_file_path=input_file_path)


def __process_sql_file(
        previous_stage_name: str,
        stage_name: str,
        input_file_path: str) \
        -> None:
    with open(input_file_path, 'r') as f:
        command_lines = f.readlines()

    output_file_path = \
        input_file_path.replace(
            previous_stage_name,
            stage_name)

    file_lines = \
        list()

    for command in command_lines:
        command = \
            replace_qualify_string_to_having(
                command_string=command)

        if command.startswith('--') or command == '\n' or command.startswith('\n--'):
            continue

        else:
            file_lines.append(
                command)

    if file_lines:
        file_writer = \
            open(output_file_path, "w")

        file_writer.writelines(file_lines)

        file_writer.close()

    else:
        if not os.path.exists(output_file_path):
            output_file_path = \
                output_file_path.replace('.sql', '.empty')

            file_writer = \
                open(output_file_path, "w")

            file_writer.writelines(list())

            file_writer.close()
