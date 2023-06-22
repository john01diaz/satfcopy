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
def orchestrate_shell_etl_files_converter_stage_05_sql(
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

    current_stage_folder_path = \
        os.path.join(
            LogFiles.folder_path,
            stage_name)

    create_new_folder(
        folder_path=current_stage_folder_path)

    sql_statements_allowed_folder_path = \
        os.path.join(
            current_stage_folder_path,
            'sql_statements_without_nominated_text')

    sql_statements_not_allowed_folder_path = \
        os.path.join(
            current_stage_folder_path,
            'sql_statements_with_nominated_text')

    __process_input_folder_children(
        splitted_files_dictionary=splitted_files_dictionary,
        sql_extension_folder_path=sql_extension_folder_path,
        relative_path_removal=relative_path_removal,
        sql_statements_allowed_folder_path=sql_statements_allowed_folder_path,
        sql_statements_not_allowed_folder_path=sql_statements_not_allowed_folder_path)

    export_table_as_dictionary_to_csv(
        table_as_dictionary=splitted_files_dictionary,
        output_folder=Folders(absolute_path_string=LogFiles.folder_path),
        output_file_base_name='stage_05_files')


def __process_input_folder_children(
        splitted_files_dictionary: dict,
        sql_extension_folder_path: str,
        relative_path_removal: str,
        sql_statements_allowed_folder_path: str,
        sql_statements_not_allowed_folder_path: str):
    for input_folder_path, dirs, filenames \
            in os.walk(sql_extension_folder_path):
        relative_path = \
            (input_folder_path + os.sep).replace(relative_path_removal, '')

        log_message(
            message='processing folder:  ' + relative_path)

        output_sql_statements_allowed_folder_path = \
            os.path.join(
                sql_statements_allowed_folder_path,
                relative_path)

        create_new_folder(
            folder_path=output_sql_statements_allowed_folder_path)

        output_sql_statements_not_allowed_folder_path = \
            os.path.join(
                sql_statements_not_allowed_folder_path,
                relative_path)

        create_new_folder(
            folder_path=output_sql_statements_not_allowed_folder_path)

        __process_files(
            splitted_files_dictionary=splitted_files_dictionary,
            filenames=filenames,
            input_folder_path=input_folder_path,
            relative_path=relative_path,
            output_sql_statements_allowed_folder_path=output_sql_statements_allowed_folder_path,
            output_sql_statements_not_allowed_folder_path=output_sql_statements_not_allowed_folder_path)


def __process_files(
        splitted_files_dictionary: dict,
        filenames: list,
        input_folder_path: str,
        relative_path: str,
        output_sql_statements_allowed_folder_path: str,
        output_sql_statements_not_allowed_folder_path: str) \
        -> None:
    for filename \
            in filenames:
        if filename[-4:] == '.sql':
            __process_file(
                filename=filename,
                input_folder_path=input_folder_path,
                relative_path=relative_path,
                output_sql_statements_allowed_folder_path=output_sql_statements_allowed_folder_path,
                output_sql_statements_not_allowed_folder_path=output_sql_statements_not_allowed_folder_path,
                splitted_files_dictionary=splitted_files_dictionary)


def __process_file(
        filename: str,
        input_folder_path: str,
        relative_path: str,
        output_sql_statements_allowed_folder_path: str,
        output_sql_statements_not_allowed_folder_path: str,
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
            output_sql_statements_allowed_folder_path=output_sql_statements_allowed_folder_path,
            output_sql_statements_not_allowed_folder_path=output_sql_statements_not_allowed_folder_path,
            splitted_files_dictionary=splitted_files_dictionary)


def __process_sql_file(
        filename: str,
        input_file_path: str,
        relative_path: str,
        output_sql_statements_allowed_folder_path: str,
        output_sql_statements_not_allowed_folder_path: str,
        splitted_files_dictionary: dict) \
        -> None:
    with open(input_file_path, 'r') as f:
        commands = f.read().split('-- COMMAND ----------\n')

    command_position = 0

    for command in commands:
        current_file_extention = \
            'sql'

        is_magic = \
            False

        if command.find('-- MAGIC') > 0:
            is_magic = \
                True

        magic_type = \
            'not_found'

        if is_magic:
            if command.find('%run') > 0:
                magic_type = 'run'
                current_file_extention = 'run'

            if command.find('%sql') > 0:
                magic_type = 'sql'

            if command.find('%python') > 0:
                magic_type = 'python'
                current_file_extention = 'py'

        new_filename = \
            filename[:-4] + '_sql_' + f'{command_position:02}'

        if is_magic:
            new_filename = new_filename + '_magic'

        if magic_type == 'sql':
            command = \
                command.replace('-- MAGIC ', '').replace('%sql', '')

        if magic_type == 'python':
            command = \
                command.replace('-- MAGIC ', '').replace('%python', '').replace('-- MAGIC', '# -- MAGIC')

        if current_file_extention == 'sql':
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

            for sql_statement in sql_statements:

                nominated_text = ['row_number()', 'qualify', 'order by']

                if any([x in sql_statement.lower() for x in nominated_text]):
                    output_file_path = \
                        os.path.join(
                            output_sql_statements_not_allowed_folder_path,
                            new_filename + '_' + f'{sql_command_position:02}' + '.' + current_file_extention)

                    with open(output_file_path, 'w') as f:
                        f.write(sql_statement)

                    add_splitted_file_to_dictionary(
                        splitted_files_dictionary=splitted_files_dictionary,
                        relative_path=relative_path,
                        filename=new_filename + '_' + f'{sql_command_position:02}' + '.' + current_file_extention,
                        source_filename=filename,
                        source_type='sql',
                        line_count=len(sql_statement.splitlines()),
                        type=current_file_extention,
                        is_magic=magic_type != 'not_found',
                        is_empty=False,
                        position_in_file=f'{command_position:02}',
                        position_in_statements=f'{sql_command_position:02}')

                elif sql_statement.isspace():
                    empty_statement_file_path = \
                        os.path.join(
                            relative_path,
                            new_filename)

                    log_message(
                        'empty sql statement command (' + f'{sql_command_position:02}' + ') in:' + empty_statement_file_path)

                    output_file_path = \
                        os.path.join(
                            output_sql_statements_not_allowed_folder_path,
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
                        is_magic=magic_type != 'not_found',
                        is_empty=True,
                        position_in_file=f'{command_position:02}',
                        position_in_statements=f'{sql_command_position:02}')

                else:
                    output_file_path = \
                        os.path.join(
                            output_sql_statements_allowed_folder_path,
                            new_filename + '_' + f'{sql_command_position:02}' + '.' + current_file_extention)

                    with open(output_file_path, 'w') as f:
                        f.write(sql_statement)

                    add_splitted_file_to_dictionary(
                        splitted_files_dictionary=splitted_files_dictionary,
                        relative_path=relative_path,
                        filename=new_filename + '_' + f'{sql_command_position:02}' + '.' + current_file_extention,
                        source_filename=filename,
                        source_type='sql',
                        line_count=len(sql_statement.splitlines()),
                        type=current_file_extention,
                        is_magic=magic_type != 'not_found',
                        is_empty=False,
                        position_in_file=f'{command_position:02}',
                        position_in_statements=f'{sql_command_position:02}')

                sql_command_position += \
                    1
        elif current_file_extention == 'py':
            if 'sql_command_position' not in locals():
                sql_command_position = \
                    0

            splitted_output_file_path = \
                output_sql_statements_allowed_folder_path.split(os.sep)

            if 'sql_statements_without_nominated_text' in splitted_output_file_path:
                index = \
                    splitted_output_file_path.index('sql_statements_without_nominated_text')

                splitted_output_file_path[index] = \
                    'python_methods'

            elif 'sql_statements_with_nominated_text' in splitted_output_file_path:
                index = \
                    splitted_output_file_path.index('sql_statements_with_nominated_text')

                splitted_output_file_path[index] = \
                    'python_methods'

            else:
                pass

            new_output_python_magic_statements_folder_path = \
                os.sep.join(splitted_output_file_path)

            new_file_full_name = \
                new_filename + '_' + f'{sql_command_position:02}' + '.' + current_file_extention

            new_output_python_magic_file_path = \
                os.path.join(
                    new_output_python_magic_statements_folder_path,
                    new_file_full_name)

            create_new_folder(
                folder_path=os.sep.join(
                    new_output_python_magic_file_path.split(os.sep)[:-1]))

            with open(new_output_python_magic_file_path, 'w') as f:
                f.write(
                    command)

            add_splitted_file_to_dictionary(
                splitted_files_dictionary=splitted_files_dictionary,
                relative_path=relative_path,
                filename=new_file_full_name,
                source_filename=filename,
                source_type='sql',
                line_count=len(command.splitlines()),
                type=current_file_extention,
                is_magic=magic_type != 'not_found',
                is_empty=False,
                position_in_file=f'{command_position:02}',
                position_in_statements=f'{sql_command_position:02}')

        command_position += \
            1
