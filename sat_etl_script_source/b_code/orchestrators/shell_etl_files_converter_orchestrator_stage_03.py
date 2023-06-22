import os
from nf_common_source.code.services.file_system_service.objects.folders import Folders
from nf_common_source.code.services.input_output_service.delimited_text.table_as_dictionary_to_csv_exporter import \
    export_table_as_dictionary_to_csv
from nf_common_source.code.services.reporting_service.reporters.log_file import LogFiles
from nf_common_source.code.services.reporting_service.reporters.log_with_datetime import log_message
from nf_common_source.code.services.reporting_service.wrappers.run_and_log_function_wrapper import run_and_log_function

from sat_etl_script_source.b_code.common.helpers.new_folder_creator import create_new_folder
from sat_etl_script_source.b_code.common.splitted_file_to_dictionary_adder import add_splitted_file_to_dictionary


@run_and_log_function
def orchestrate_shell_etl_files_converter_stage_03_py(
        input_root_folder: Folders,
        previous_stage_name: str,
        stage_name: str) \
        -> None:
    splitted_files_dictionary = \
        dict()

    log_message(
        message='extract python methods from sql files')

    log_message(
        message='input filepath:' + input_root_folder.absolute_path_string)

    py_extension_folder_path = \
        os.path.join(
            LogFiles.folder_path,
            previous_stage_name,
            'py_ext')

    relative_path_removal = \
        py_extension_folder_path + os.sep

    stage_04_folder_path = \
        os.path.join(
            LogFiles.folder_path,
            stage_name)

    py_methods_folder_path = \
        os.path.join(
            stage_04_folder_path,
            'py_ext')

    __process_input_folder_children(
        splitted_files_dictionary=splitted_files_dictionary,
        py_extension_folder_path=py_extension_folder_path,
        py_methods_folder_path=py_methods_folder_path,
        relative_path_removal=relative_path_removal)

    export_table_as_dictionary_to_csv(
        table_as_dictionary=splitted_files_dictionary,
        output_folder=Folders(absolute_path_string=LogFiles.folder_path),
        output_file_base_name='py_method_04_files')


def __process_input_folder_children(
        splitted_files_dictionary: dict,
        py_extension_folder_path: str,
        py_methods_folder_path: str,
        relative_path_removal: str) \
        -> None:
    for input_folder_path, dirs, filenames \
            in os.walk(py_extension_folder_path):
        relative_path = \
            (input_folder_path + os.sep).replace(relative_path_removal, '')

        log_message(
            message='processing folder:  ' + relative_path)

        output_py_methods_folder_path = \
            os.path.join(
                py_methods_folder_path,
                relative_path)

        create_new_folder(
            folder_path=output_py_methods_folder_path)

        __process_files(
            filenames=filenames,
            input_folder_path=input_folder_path,
            relative_path=relative_path,
            output_py_methods_folder_path=output_py_methods_folder_path,
            splitted_files_dictionary=splitted_files_dictionary)


def __process_files(
        filenames: list,
        input_folder_path: str,
        relative_path: str,
        output_py_methods_folder_path: str,
        splitted_files_dictionary: dict) \
        -> None:
    for filename \
            in filenames:
        if filename[-3:] == '.py':
            __process_file(
                filename=filename,
                input_folder_path=input_folder_path,
                relative_path=relative_path,
                output_py_methods_folder_path=output_py_methods_folder_path,
                splitted_files_dictionary=splitted_files_dictionary)


def __process_file(
        filename: str,
        input_folder_path: str,
        relative_path: str,
        output_py_methods_folder_path: str,
        splitted_files_dictionary: dict) \
        -> None:
    log_message(
        message='processing file:      ' + filename)

    input_file_path = \
        os.path.join(
            input_folder_path,
            filename)

    __process_py_file(
            filename=filename,
            input_file_path=input_file_path,
            relative_path=relative_path,
            output_py_methods_folder_path=output_py_methods_folder_path,
            splitted_files_dictionary=splitted_files_dictionary)


def __process_py_file(
        filename: str,
        input_file_path: str,
        relative_path: str,
        output_py_methods_folder_path: str,
        splitted_files_dictionary: dict) \
        -> None:
    with open(input_file_path, 'r') as f:
        commands = f.read().split('# COMMAND ----------\n')

    command_position = \
        0

    for command \
            in commands:
        extension = \
            'py'

        new_filename = \
            filename[:-3] + '_py_' + f'{command_position:02}'

        if command.isspace():
            empty_statement_file_path = \
                os.path.join(
                    relative_path,
                    new_filename)

            log_message(
                'empty py method command in:' + empty_statement_file_path)

            output_file_path = \
                os.path.join(
                    output_py_methods_folder_path,
                    new_filename + '.empty')

            with open(output_file_path, 'w') as f:
                f.write(command)

            add_splitted_file_to_dictionary(
                splitted_files_dictionary=splitted_files_dictionary,
                relative_path=relative_path,
                filename=new_filename + '.empty',
                source_filename=filename,
                source_type='sql',
                line_count=len(command.splitlines()),
                type='empty',
                is_empty=True,
                position_in_file=f'{command_position:02}',
                position_in_statements='')

        else:
            output_file_path = \
                os.path.join(
                    output_py_methods_folder_path,
                    new_filename + '.' + extension)

            with open(output_file_path, 'w') as f:
                f.write(command)

            add_splitted_file_to_dictionary(
                splitted_files_dictionary=splitted_files_dictionary,
                relative_path=relative_path,
                filename=new_filename + '.' + extension,
                source_filename=filename,
                source_type='py',
                line_count=len(command.splitlines()),
                type=extension,
                is_empty=False,
                position_in_file=f'{command_position:02}',
                position_in_statements='')

        command_position += \
            1
