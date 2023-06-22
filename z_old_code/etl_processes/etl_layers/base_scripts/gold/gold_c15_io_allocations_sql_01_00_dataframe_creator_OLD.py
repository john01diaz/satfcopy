import pandas


def create_dataframe_gold_c15_io_allocations_sql_01_00(
        s_io_allocations_dataframe: pandas.DataFrame,
        database_names_dataframe: pandas.DataFrame) \
        -> pandas.DataFrame:
    columns_to_select = \
        [
            'Tag_Number',
            'Parent_Equipment_No',
            'IOType',
            'Equipment_No',
            'CatalogueNo',
            'ChannelNumber'
        ]

    class_values = \
        [
            'Instrumentation',
            'Inst(Shared)',
            'Elec(Shared)'
        ]

    filtered_dataframe = \
        filter_class_and_database(
            s_io_allocations_dataframe,
            class_values,
            database_names_dataframe)

    io_allocations_dataframe = \
        select_distinct_columns(
            filtered_dataframe,
            columns_to_select)

    return \
        io_allocations_dataframe


def rename_columns(
        input_dataframe: pandas.DataFrame,
        rename_dict: dict):
    return \
        input_dataframe.rename(
            columns=rename_dict)


def filter_class_and_database(
        input_dataframe: pandas.DataFrame,
        class_values,
        databases_df) \
        -> pandas.DataFrame:
    filtered_by_class = \
        input_dataframe[input_dataframe['Class'].isin(
            class_values)]

    database_names = \
        databases_df['Database_name'].tolist()

    filtered_by_database = \
        filtered_by_class[filtered_by_class['database_name'].isin(
            database_names)]

    return \
        filtered_by_database


def select_distinct_columns(
        input_dataframe,
        columns: list) \
        -> pandas.DataFrame:
    return \
        input_dataframe[columns].drop_duplicates()
