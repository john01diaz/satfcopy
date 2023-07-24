def run_10_s_io_catalogue_sql_06_00_python_only(
        input_tables: dict) \
        -> None:
    input_dataframe = \
        input_tables['TBD']

    output_table = \
        input_dataframe[input_dataframe['NoOfPoints'] < 100]

    return \
        output_table
