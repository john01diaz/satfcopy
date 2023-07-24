def run_11_s_loop_index_sql_02_00_python_only(
        input_tables: dict) \
        -> None:
    loop_index_dataframe = \
        input_tables['TBD']

    column_renames = \
        {
            'loop_database_name': 'database_name',
            'loop_dynamic_class': 'dynamic_class',
            'loop_object_identifier': 'object_identifier'
        }

    output_table = \
        loop_index_dataframe.rename(
            columns=column_renames)

    return \
        output_table
