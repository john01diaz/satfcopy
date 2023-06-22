import pandas


def create_dataframe_gold_c12_terminals_sql_01_00(
        terminals_dataframe: pandas.DataFrame,
        database_names_dataframe: pandas.DataFrame) \
        -> pandas.DataFrame:
    # Note: added by hand to filter the set of columns
    columns_to_keep = \
        [
            'Parent_Equipment_No',
            'Equipment_No',
            'Marking',
            'Sequence'
        ]

    class_filter = \
        [
            'Instrumentation',
            'Inst(Shared)',
            'Elec(Shared)'
        ]

    filtered_terminals = \
        __filter_terminals(
            terminals_dataframe,
            class_filter,
            database_names_dataframe)

    ranked_terminals = \
        __rank_terminals(
            filtered_terminals)

    # Note: added by hand to filter the set of columns
    ranked_terminals = \
        ranked_terminals.filter(
            items=columns_to_keep)

    return \
        ranked_terminals


def __filter_terminals(
        terminals_df,
        class_filter,
        database_names):
    # Note: database_names was converted to a list: list(database_names['Database_name'])
    filtered_terminals = \
        terminals_df[
            (terminals_df['Class'].isin(
                class_filter)) &
            (terminals_df['database_name'].isin(
                list(database_names['Database_name'])))]

    return \
        filtered_terminals


def __rank_terminals(
        filtered_terminals):
    # Note: added a line to create a copy of the input dataframe
    filtered_terminals_copy = \
        filtered_terminals.copy()

    filtered_terminals_copy['Sequence'] = \
        filtered_terminals.groupby(
            ['Parent_Equipment_No', 'Equipment_No']
            )['Marking'].rank(
                method='dense')

    return \
        filtered_terminals_copy
