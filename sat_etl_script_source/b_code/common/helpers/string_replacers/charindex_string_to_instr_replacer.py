import os
import re
from nf_common_source.code.services.reporting_service.reporters.log_with_datetime import log_message


def replace_charindex_to_instr(
        line_index: int,
        input_file_path: str,
        input_sql_command: str) \
        -> str:
    if 'charindex' in input_sql_command.lower():
        input_sql_command = \
            __get_charindex_parameters(
                charindex_value='charindex',
                regex_to_catch_content_between_charindex=r'{0}\([^\)]+\)',
                line_index=line_index,
                input_file_path=input_file_path,
                sql_command=input_sql_command)

    return \
        input_sql_command


def __get_charindex_parameters(
        charindex_value: str,
        regex_to_catch_content_between_charindex: str,
        line_index: int,
        input_file_path: str,
        sql_command: str) \
        -> str:
    matched_groups = \
        re.findall(
            regex_to_catch_content_between_charindex.format(charindex_value),
            sql_command,
            re.IGNORECASE)

    if not matched_groups:
        return \
            sql_command

    for matched_string \
            in matched_groups:
        matched_group = \
            matched_string.split('(')[-1].replace(')', str())

        if matched_string not in sql_command:
            continue

        sql_command = \
            __replace_charindex_match(
                matched_string=matched_string,
                matched_group=matched_group,
                line_index=line_index,
                input_file_path=input_file_path,
                sql_command=sql_command)

    return \
        sql_command


def __replace_charindex_match(
        matched_string: str,
        matched_group: str,
        line_index: int,
        input_file_path: str,
        sql_command: str) \
        -> str:
    charindex_parameters = \
        matched_group.split(',')

    input_substring_value = \
        charindex_parameters[0]

    input_string_variable = \
        charindex_parameters[1]

    new_matched_string = \
        'instr(' + input_string_variable + ',' + input_substring_value + ')'

    position = \
        sql_command.find(
            matched_string)

    if not position:
        raise \
            Exception

    sql_command = \
        sql_command.replace(
            matched_string,
            new_matched_string)

    log_message(
        message='A match was found: {0} replaced to {1} - Input file name: {2} - Line {3} and position: {4}'.format(
            matched_string,
            new_matched_string,
            input_file_path.split(os.sep)[-1],
            str(line_index),
            position))

    return \
        sql_command
