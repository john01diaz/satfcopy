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
def orchestrate_shell_etl_files_converter_stage_04_sql(
        input_root_folder: Folders,
        previous_stage_name: str,
        stage_name: str) \
        -> None:
    splitted_files_dictionary = \
        dict()

    log_message(
        message='input filepath:' + input_root_folder.absolute_path_string)

    sql_extension_folder_path = \
        os.path.join(
            LogFiles.folder_path,
            previous_stage_name,
            'sql_ext')

    relative_path_removal = \
        sql_extension_folder_path + os.sep

    previous_stage_folder_path = \
        os.path.join(
            LogFiles.folder_path,
            stage_name)

    create_new_folder(
        folder_path=previous_stage_folder_path)

    sql_extension_files_folder_path = \
        os.path.join(
            previous_stage_folder_path,
            'sql_ext')

    __process_input_folder_children(
        splitted_files_dictionary=splitted_files_dictionary,
        sql_extension_folder_path=sql_extension_folder_path,
        relative_path_removal=relative_path_removal,
        sql_extension_files_folder_path=sql_extension_files_folder_path)

    export_table_as_dictionary_to_csv(
        table_as_dictionary=splitted_files_dictionary,
        output_folder=Folders(absolute_path_string=LogFiles.folder_path),
        output_file_base_name='stage_04_files')


def __process_input_folder_children(
        splitted_files_dictionary: dict,
        sql_extension_folder_path: str,
        relative_path_removal: str,
        sql_extension_files_folder_path: str) \
        -> None:
    for input_folder_path, dirs, filenames \
            in os.walk(sql_extension_folder_path):
        relative_path = \
            (input_folder_path + os.sep).replace(
                relative_path_removal,
                str())

        log_message(
            message='processing folder:  ' + relative_path)

        output_sql_extension_files_folder_path = \
            os.path.join(
                sql_extension_files_folder_path,
                relative_path)

        create_new_folder(
            folder_path=output_sql_extension_files_folder_path)

        __process_files(
            splitted_files_dictionary=splitted_files_dictionary,
            filenames=filenames,
            input_folder_path=input_folder_path,
            relative_path=relative_path,
            output_sql_extension_files_folder_path=output_sql_extension_files_folder_path)


def __process_files(
        splitted_files_dictionary: dict,
        filenames: list,
        input_folder_path: str,
        relative_path: str,
        output_sql_extension_files_folder_path: str) \
        -> None:
    for filename \
            in filenames:
        if filename[-4:] == '.sql':
            __process_file(
                filename=filename,
                input_folder_path=input_folder_path,
                relative_path=relative_path,
                output_sql_extension_files_folder_path=output_sql_extension_files_folder_path,
                splitted_files_dictionary=splitted_files_dictionary)


def __process_file(
        filename: str,
        input_folder_path: str,
        relative_path: str,
        output_sql_extension_files_folder_path: str,
        splitted_files_dictionary: dict) \
        -> None:
    log_message(
        message='processing file:      ' + filename)

    input_file_path = \
        os.path.join(
            input_folder_path,
            filename)

    __process_sql_file(
            filename=filename,
            input_file_path=input_file_path,
            relative_path=relative_path,
            output_sql_extension_files_folder_path=output_sql_extension_files_folder_path,
            splitted_files_dictionary=splitted_files_dictionary)


def __process_sql_file(
        filename: str,
        input_file_path: str,
        relative_path: str,
        output_sql_extension_files_folder_path: str,
        splitted_files_dictionary: dict) \
        -> None:
    with open(input_file_path, 'r') as f:
        commands = f.read().split('-- COMMAND ----------\n')

    command_position = 0

    for command \
            in commands:
        extention = \
            'sql'

        new_filename = \
            filename[:-4] + '_sql_' + f'{command_position:02}'

        if extention == 'sql':
            sql_statements = \
                command.split(';')

            if len(sql_statements) > 1:
                multi_statement_file_path = \
                        os.path.join(
                            relative_path,
                            new_filename)

                log_message(
                    'multi sql statement command in:' + multi_statement_file_path)

            sql_command_position = \
                0

            __process_sql_file_sql_statements(
                splitted_files_dictionary=splitted_files_dictionary,
                sql_statements=sql_statements,
                filename=filename,
                extention=extention,
                relative_path=relative_path,
                command_position=command_position,
                new_filename=new_filename,
                sql_command_position=sql_command_position,
                output_sql_extension_files_folder_path=output_sql_extension_files_folder_path)

        command_position += \
            1


def __process_sql_file_sql_statements(
        splitted_files_dictionary: dict,
        sql_statements: list,
        filename: str,
        extention: str,
        relative_path: str,
        command_position: int,
        new_filename: str,
        sql_command_position: int,
        output_sql_extension_files_folder_path: str):
    for sql_statement \
            in sql_statements:
        if sql_statement.isspace():
            empty_statement_file_path = \
                os.path.join(
                    relative_path,
                    new_filename)

            log_message(
                'empty sql statement command (' + f'{sql_command_position:02}' + ') in:' + empty_statement_file_path)

            output_file_path = \
                os.path.join(
                    output_sql_extension_files_folder_path,
                    new_filename + '_' + f'{sql_command_position:02}' + '.empty')

            with open(output_file_path, 'w') as f:
                f.write(sql_statement)

            add_splitted_file_to_dictionary(
                splitted_files_dictionary=splitted_files_dictionary,
                relative_path=relative_path,
                filename=new_filename + '_' + f'{sql_command_position:02}' + '.empty',
                source_filename=filename,
                source_type='sql',
                line_count=len(sql_statement.splitlines()),
                type='empty',
                is_empty=True,
                position_in_file=f'{command_position:02}',
                position_in_statements=f'{sql_command_position:02}')

        else:
            output_file_path = \
                os.path.join(
                    output_sql_extension_files_folder_path,
                    new_filename + '_' + f'{sql_command_position:02}' + '.' + extention)

            with open(output_file_path, 'w') as f:
                f.write(sql_statement)

            add_splitted_file_to_dictionary(
                splitted_files_dictionary=splitted_files_dictionary,
                relative_path=relative_path,
                filename=new_filename + '_' + f'{sql_command_position:02}' + '.' + extention,
                source_filename=filename,
                source_type='sql',
                line_count=len(sql_statement.splitlines()),
                type=extention,
                is_empty=False,
                position_in_file=f'{command_position:02}',
                position_in_statements=f'{sql_command_position:02}')

        sql_command_position += \
            1
