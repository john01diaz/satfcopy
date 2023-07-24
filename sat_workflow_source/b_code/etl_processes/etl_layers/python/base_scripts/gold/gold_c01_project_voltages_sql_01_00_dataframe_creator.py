from sat_workflow_source.b_code.etl_schemas.gold_stage.project_voltages import Project_Voltages
#from sat_workflow_source.gpt4_code.raw_python_generation.gold_stage.a_raw.a01_Project_Voltages_sql_01_00 import \
#   CS_Layer_Loop_Loop_elements_FilteredForProjectVoltages

from enum import Enum
#import pandas

# Define your Enums
class DatabaseNames(Enum):
    DATABASE_NAME = 'database_name'


class Project_Voltages(Enum):
    VOLTAGE_TYPE = 'voltage_type'
    VOLTAGE = 'voltage'


class CS_Layer_Loop_Loop_elements_FilteredFor01ProjectVoltagesSql0100Sql(Enum):
    LOOP_ELEMENT_CS_VOLTAGE_TYPE = 'loop_element_cs_voltage_type'
    CS_VOLTAGE = 'cs_voltage'


def __replace_default_isnull_strings(dataframe):
    """
    Function to replace 'b%default%isnull' strings with None in a pandas DataFrame
    """
    return dataframe.replace('b%default%isnull', None)


def __remove_null_values(dataframe, column_to_filter):
    """
    Function to remove null values from a specific column in a pandas DataFrame
    """
    return dataframe[dataframe[column_to_filter].notnull()]


def __filter_values(dataframe, column_to_filter, values_to_exclude):
    """
    Function to exclude specific values from a specific column in a pandas DataFrame
    """
    return dataframe[~dataframe[column_to_filter].isin(values_to_exclude)]


def __rename_columns(dataframe, rename_mapping):
    """
    Function to rename columns in a pandas DataFrame based on a provided mapping
    """
    return dataframe.rename(columns=rename_mapping)


def __remove_duplicates(dataframe):
    """
    Function to remove duplicate rows from a pandas DataFrame
    """
    return dataframe.drop_duplicates()


def create_silver_UT_xxxx(input_tables):
    """
    Orchestrating function to translate the SQL operations into Python
    It assumes that the input_tables parameter is a dictionary of pandas DataFrames
    """
    # Load dataframe from input_tables dictionary
    table_name = CS_Layer_Loop_Loop_elements_FilteredFor01ProjectVoltagesSql0100Sql
    dataframe = input_tables[table_name]

    # Replace 'b%default%isnull' strings with None
    dataframe = __replace_default_isnull_strings(dataframe)

    # Filter values
    column_to_filter = CS_Layer_Loop_Loop_elements_FilteredFor01ProjectVoltagesSql0100Sql.LOOP_ELEMENT_CS_VOLTAGE_TYPE.value
    values_to_exclude = ['_empty_', ' ', '+0','b%default%isnull']
    dataframe = __filter_values(dataframe, column_to_filter, values_to_exclude)

    # Remove null values
    dataframe = __remove_null_values(dataframe, column_to_filter)

    # Rename columns
    rename_mapping = {
        CS_Layer_Loop_Loop_elements_FilteredFor01ProjectVoltagesSql0100Sql.LOOP_ELEMENT_CS_VOLTAGE_TYPE.value: Project_Voltages.VOLTAGE_TYPE.value,
        CS_Layer_Loop_Loop_elements_FilteredFor01ProjectVoltagesSql0100Sql.CS_VOLTAGE.value: Project_Voltages.VOLTAGE.value
    }
    dataframe = __rename_columns(dataframe, rename_mapping)

    # Remove duplicates
    dataframe = __remove_duplicates(dataframe)

    return dataframe
