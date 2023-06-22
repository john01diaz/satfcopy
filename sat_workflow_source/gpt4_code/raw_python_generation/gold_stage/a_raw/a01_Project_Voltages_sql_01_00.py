from enum import Enum
import pandas


class DatabaseNames(Enum):
    DATABASE_NAME = 'database_name'


class CS_Layer_Loop_Loop_elements_FilteredForProjectVoltages(Enum):
    LOOP_ELEMENT_CS_VOLTAGE_TYPE = 'loop_element_cs_voltage_type'
    CS_VOLTAGE = 'cs_voltage'


class Project_Voltages(Enum):
    VOLTAGE_TYPE = 'voltage_type'
    VOLTAGE = 'voltage'


def rename_columns(dataframe, new_column_names):
    return dataframe.rename(columns=new_column_names)


def filter_non_null_rows(dataframe, column):
    return dataframe[dataframe[column].notnull()]


def filter_specific_value(dataframe, column, value_to_exclude):
    return dataframe[dataframe[column] != value_to_exclude]


def select_columns(dataframe, column_names):
    return dataframe[column_names].drop_duplicates()


def project_voltages(cs_layer_loop_loop_elements_df, vw_database_names_df):
    # Define constant values
    empty_string = '_empty_'
    blank_string = ' '
    zero_string = '+0'

    # Define column mappings
    column_mapping = {
        CS_Layer_Loop_Loop_elements_FilteredForProjectVoltages.LOOP_ELEMENT_CS_VOLTAGE_TYPE.value: Project_Voltages.VOLTAGE_TYPE.value,
        CS_Layer_Loop_Loop_elements_FilteredForProjectVoltages.CS_VOLTAGE.value: Project_Voltages.VOLTAGE.value
    }

    # Renaming columns
    dataframe_renamed = rename_columns(cs_layer_loop_loop_elements_df, column_mapping)

    # Filtering rows
    dataframe_filtered_non_null = filter_non_null_rows(dataframe_renamed, Project_Voltages.VOLTAGE_TYPE.value)
    dataframe_filtered_empty = filter_specific_value(dataframe_filtered_non_null, Project_Voltages.VOLTAGE_TYPE.value, empty_string)
    dataframe_filtered_blank = filter_specific_value(dataframe_filtered_empty, Project_Voltages.VOLTAGE_TYPE.value, blank_string)
    dataframe_filtered_zero = filter_specific_value(dataframe_filtered_blank, Project_Voltages.VOLTAGE.value, zero_string)

    # Selecting columns in specified order
    column_order = [Project_Voltages.VOLTAGE_TYPE.value, Project_Voltages.VOLTAGE.value]
    final_dataframe = select_columns(dataframe_filtered_zero, column_order)

    return final_dataframe
