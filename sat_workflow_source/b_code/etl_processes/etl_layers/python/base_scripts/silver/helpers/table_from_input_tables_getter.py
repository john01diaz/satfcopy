from pandas import DataFrame


def get_table_from_input_tables(
        input_tables: dict,
        table_name: str) \
        -> DataFrame:
    if table_name not in input_tables:
        raise \
            KeyError(
                f'Table {table_name} not found in loaded input_tables')

    return \
        input_tables[table_name]
