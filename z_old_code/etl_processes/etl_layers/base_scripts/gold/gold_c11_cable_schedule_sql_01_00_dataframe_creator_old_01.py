import pandas


def create_dataframe_gold_c11_cable_schedule_sql_01_00(
        database_names_dataframe: pandas.DataFrame,
        cable_schedule_dataframe: pandas.DataFrame) \
        -> pandas.DataFrame:
    cable_schedule_dataframe = \
        get_cable_schedule_data(
            cable_schedule_dataframe,
            database_names_dataframe)

    # Note: 'writeable' is not a valid flag - valid flags are: 'copy' and 'allows_duplicate_labels'
    # cable_schedule_dataframe = \
    #     cable_schedule_dataframe.set_flags(
    #         writeable=False)

    return \
        cable_schedule_dataframe


def get_cable_schedule_data(
        cable_schedule_dataframe: pandas.DataFrame,
        database_dataframe: pandas.DataFrame) \
        -> pandas.DataFrame:
    cable_schedule_data = \
        cable_schedule_dataframe[cable_schedule_dataframe['database_name'].isin(
            database_dataframe['Database_name'])]

    cable_schedule_data = \
        cable_schedule_data.drop_duplicates()

    return \
        cable_schedule_data
