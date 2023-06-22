import re


def rename_tables_in_sql_script(
        base_sql_script_as_string: str,
        sql_table_names_mapping_dictionary: dict) \
        -> str:
    renamed_sql_script_as_string = \
        base_sql_script_as_string

    for original_table_name, refactored_table_name \
            in sql_table_names_mapping_dictionary.items():
        renamed_sql_script_as_string = \
            re.sub(
                original_table_name,
                refactored_table_name,
                renamed_sql_script_as_string,
                flags=re.I)

    return \
        renamed_sql_script_as_string
