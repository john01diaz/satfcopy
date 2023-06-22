import re
import pandas


def create_dataframe_gold_c02_instrument_type_catalogue_sql_01_00(
        cs_layer_loop_loop_elements_dataframe: pandas.DataFrame,
        database_names: pandas.DataFrame) \
        -> pandas.DataFrame:
    filtered_instrument_data = \
        __filter_instrument_data(
            cs_layer_loop_loop_elements_dataframe,
            database_names)

    unique_instrument_types = \
        __get_unique_instrument_types(
            filtered_instrument_data)

    instrument_type_catalogue_dataframe = \
        __sort_by_type(
            unique_instrument_types)

    return \
        instrument_type_catalogue_dataframe


def __extract_type(
        string: str) \
        -> str:
    match = \
        re.match(
            r'[A-Za-z]+',
            string)

    return \
        match.group(
            0) if match else None


def __filter_instrument_data(
        dataframe: pandas.DataFrame,
        database_names: pandas.DataFrame) \
        -> pandas.DataFrame:
    conditions = (
            dataframe['CS_location_full_designation'].isnull()
            & dataframe['loop_element_database_name'].isin(
                database_names['Database_name'])  # Assuming database_names is defined
            & dataframe['CS_device_type'].notnull()
            & dataframe['CS_device_type'].str.strip().ne(""))

    return \
        dataframe[conditions].copy()


def __get_unique_instrument_types(
        dataframe: pandas.DataFrame) \
        -> pandas.DataFrame:
    dataframe['Type'] = \
        dataframe['CS_loop_element_id'].apply(
            __extract_type)

    dataframe['InstrumentType'] = \
        dataframe['CS_device_type'].str.upper().str.strip()

    unique_instrument_types = \
        dataframe[['Type', 'InstrumentType']].drop_duplicates()

    return \
        unique_instrument_types.copy()


def __sort_by_type(
        dataframe: pandas.DataFrame) \
        -> pandas.DataFrame:
    return \
        dataframe.sort_values(
            by=['Type']).copy()
