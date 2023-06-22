import os
from nf_common_source.code.services.file_system_service.objects.folders import Folders
from nf_common_source.code.services.reporting_service.reporters.log_file import LogFiles
from nf_common_source.code.services.reporting_service.reporters.log_with_datetime import log_message
from nf_common_source.code.services.reporting_service.wrappers.run_and_log_function_wrapper import run_and_log_function

from sat_etl_script_source.b_code.common.helpers.new_folder_creator import create_new_folder


@run_and_log_function
def orchestrate_shell_etl_files_converter_stage_02(
        input_root_folder: Folders,
        previous_stage_name: str,
        stage_name: str):
    log_message(
        message='de-magic sql and python files')

    log_message(
        message='input filepath:' + input_root_folder.absolute_path_string)

    stage_folder_path = \
        os.path.join(
            LogFiles.folder_path,
            stage_name)

    create_new_folder(
        folder_path=stage_folder_path)

    input_folder = \
        Folders(
            absolute_path_string=os.path.join(
                LogFiles.folder_path,
                previous_stage_name))

    relative_path_removal = \
        input_root_folder.parent_absolute_path_string + \
        os.sep

    __process_input_root_folder_children(
        input_root_folder=input_folder,
        previous_stage_name=previous_stage_name,
        stage_name=stage_name,
        relative_path_removal=relative_path_removal)


def __process_input_root_folder_children(
        previous_stage_name: str,
        stage_name: str,
        input_root_folder: Folders,
        relative_path_removal: str) \
        -> None:
    for input_folder_path, dirs, filenames \
            in os.walk(input_root_folder.absolute_path_string):
        relative_path = \
            input_folder_path.replace(
                relative_path_removal,
                str())

        log_message(
            message='processing folder:  ' + relative_path)

        if filenames:
            __process_folder_children_files(
                previous_stage_name=previous_stage_name,
                stage_name=stage_name,
                filenames=filenames,
                input_folder_path=input_folder_path)

        else:
            log_message(
                message='processed folder: ' + relative_path + ' - DONE.')


def __process_folder_children_files(
        previous_stage_name: str,
        stage_name: str,
        filenames: list,
        input_folder_path: str):
    for filename \
            in filenames:
        current_stage_input_folder_path = \
            input_folder_path.replace(
                previous_stage_name,
                stage_name)

        if not os.path.exists(current_stage_input_folder_path):
            os.makedirs(
                current_stage_input_folder_path)

        input_file_path = \
            os.path.join(
                input_folder_path,
                filename)

        new_input_file_path = \
            os.path.join(
                current_stage_input_folder_path,
                filename)

        __process_file(
            input_file_path=input_file_path,
            new_input_file_path=new_input_file_path)


def __process_file(
        input_file_path: str,
        new_input_file_path: str):
    non_magic_commands = \
        list()

    with open(input_file_path, 'r') as file_reader:
        lines = \
            file_reader.read().split('\n')

    for line \
            in lines:
        line = \
            __remove_magic_code_sql_file(
                line=line)

        line = \
            __remove_magic_code_python_file(
                command=line)

        non_magic_commands.append(
            line)

    with open(new_input_file_path, 'w') as output:
        for non_magic_command in non_magic_commands:
            output.write(
                str(non_magic_command) + '\n')


def __remove_magic_code_sql_file(
        line: str) \
        -> str:
    if '-- MAGIC %' in line:
        pass

    elif '-- MAGIC ' in line or '-- MAGIC' in line:
        line = \
            line.replace(
                '-- MAGIC',
                str())

    else:
        pass

    return \
        line


def __remove_magic_code_python_file(
        command: str) \
        -> str:
    if '# MAGIC %' in command:
        pass

    elif '# MAGIC ' in command or '# MAGIC' in command:
        command = \
            command.replace(
                '# MAGIC',
                str())

    else:
        pass

    return \
        command
