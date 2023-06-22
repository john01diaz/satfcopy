from pandas import DataFrame


def filter_table_columns(
        input_table: DataFrame,
        columns_to_include: list) \
        -> DataFrame:

    filtered_table = \
        input_table[columns_to_include]
    
    return \
        filtered_table
