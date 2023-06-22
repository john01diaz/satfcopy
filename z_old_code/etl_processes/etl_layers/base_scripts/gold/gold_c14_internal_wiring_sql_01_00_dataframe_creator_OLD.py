import pandas


def create_dataframe_gold_c14_internal_wiring_sql_01_00(
        s_internal_wiring_dataframe: pandas.DataFrame,
        database_names_dataframe: pandas.DataFrame) \
        -> pandas.DataFrame:
    # Constants
    CLASSES = \
        [
            'Instrumentation',
            'Inst(Shared)',
            'Elec(Shared)'
        ]

    COLUMN_RENAMES = \
        {
            'From_Parent_Equipment_No': 'From_Parent_Equipment_No',
            'From_Compartment': 'From_Compartment',
            'From_Equipment': 'From_Equipment',
            'From_Wire_Link': 'From_Wire_Link',
            'From_Marking': 'From_Marking',
            'From_Left_Right': 'From_Left_Right',
            'To_Parent_Equipment_No': 'To_Parent_Equipment_No',
            'To_Compartment': 'To_Compartment',
            'To_Equipment_No': 'To_Equipment_No',
            'To_Wire_Link': 'To_Wire_Link',
            'To_Marking': 'To_Marking',
            'To_Left_Right': 'To_Left_Right'
        }

    filtered_data = \
        __filter_classes(
            s_internal_wiring_dataframe,
            CLASSES)

    unique_database_names = \
        set(
            database_names_dataframe['Database_name'].unique())

    filtered_data = \
        __filter_database_names(
            filtered_data,
            unique_database_names)

    selected_columns_data = \
        filtered_data[list(
            COLUMN_RENAMES.keys())].drop_duplicates()

    internal_wiring_dataframe = \
        __rename_columns(
            selected_columns_data,
            COLUMN_RENAMES)

    return \
        internal_wiring_dataframe


def __filter_classes(
        input_dataframe: pandas.DataFrame,
        classes) \
        -> pandas.DataFrame:
    return \
        input_dataframe[input_dataframe['Class'].isin(
            classes)]


def __filter_database_names(
        input_dataframe,
        database_names) \
        -> pandas.DataFrame:
    return \
        input_dataframe[input_dataframe['database_name'].isin(
            database_names)]


def __rename_columns(
        input_dataframe: pandas.DataFrame,
        column_dict: dict) \
        -> pandas.DataFrame:
    return \
        input_dataframe.rename(
            columns=column_dict)
