import pandas


def create_dataframe_gold_c01_project_voltages_sql_01_00(
        cs_layer_loop_loop_elements_dataframe: pandas.DataFrame) \
        -> pandas.DataFrame:
    unique_voltage_data = \
        __get_unique_voltage_data(
            cs_layer_loop_loop_elements_dataframe)

    column_name_mapping = \
        {
            'loop_element_cs_voltage_type': 'Voltage_Type',
            'CS_voltage': 'Voltage'
            }

    process_voltages_dataframe = \
        __rename_dataframe_columns(
            unique_voltage_data,
            column_name_mapping)

    return \
        process_voltages_dataframe


def __filter_voltage_data(
        dataframe: pandas.DataFrame) \
        -> pandas.DataFrame:
    conditions = (
        dataframe['loop_element_cs_voltage_type'].notnull()
        & (dataframe['loop_element_cs_voltage_type'] != '_empty_')
        & (dataframe['loop_element_cs_voltage_type'] != " ")
        & (dataframe['CS_voltage'] != '+0'))

    return \
        dataframe[conditions].copy()


def __get_unique_voltage_data(
        dataframe: pandas.DataFrame) \
        -> pandas.DataFrame:
    return \
        dataframe[['loop_element_cs_voltage_type', 'CS_voltage']].drop_duplicates().copy()


def __rename_dataframe_columns(
        dataframe: pandas.DataFrame,
        column_dictionary: dict) \
        -> pandas.DataFrame:
    return \
        dataframe.rename(
            columns=column_dictionary).copy()
