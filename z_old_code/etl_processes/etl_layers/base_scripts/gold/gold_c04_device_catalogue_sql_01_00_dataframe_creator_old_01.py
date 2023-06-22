import pandas


def create_dataframe_gold_c04_device_catalogue_sql_01_00(
        s_device_catalogue_dataframe: pandas.DataFrame,
        database_names_dataframe: pandas.DataFrame) \
        -> pandas.DataFrame:
    filtered_df = \
        s_device_catalogue_dataframe[
            # (s_device_catalogue_dataframe['Catalogue_RNT'] == 1) &
            (s_device_catalogue_dataframe['class'].isin(['Instrumentation', 'Inst(Shared)', 'Elec(Shared)']))]

    filtered_database_names = \
        __filter_database_names(
            filtered_df,
            database_names_dataframe)

    required_columns = \
        [
            "allowuse",
            "type",
            "description",
            "manufacturer",
            "modelno",
            "class",
            "left",
            "right",
            "left_marking",
            "right_marking",
            "symbol_name",
            "product_key",
            "loop_number",
            "tag_number",
            "document_number"
        ]

    device_catalogue_dataframe = \
        filtered_database_names[required_columns].drop_duplicates()

    return \
        device_catalogue_dataframe


def __filter_database_names(
        dataframe: pandas.DataFrame,
        database_names: pandas.DataFrame) \
        -> pandas.DataFrame:
    return \
        dataframe[dataframe['database_name'].isin(
            database_names['database_name'])]
