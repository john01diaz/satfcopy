from sat_workflow_source.b_code.etl_schemas.bronze_stage.filtered\
    .cs_layer_loop_loop_elements_filtered_for_01_Project_Voltages_sql_01_00_sql import \
    CS_Layer_Loop_Loop_elements_FilteredFor01ProjectVoltagesSql0100Sql
from sat_workflow_source.b_code.etl_schemas.gold_stage.project_voltages import Project_Voltages


def rename_columns(
        dataframe,
        new_column_names):
    return dataframe.rename(
        columns=new_column_names)


def filter_non_null_rows(
        dataframe,
        column):
    return dataframe[dataframe[column].notnull()]


def filter_specific_value(
        dataframe,
        column,
        value_to_exclude):
    return dataframe[dataframe[column] != value_to_exclude]


def select_columns(
        dataframe,
        column_names):
    return dataframe[column_names].drop_duplicates()


def create_dataframe_gold_c01_project_voltages_sql_01_00_OLD(
        input_tables: dict):
    cs_layer_loop_loop_elements_dataframe = \
        input_tables['Sigraph.CS_Layer_Loop_Loop_elements']

    # Define constant values
    empty_string = '_empty_'
    blank_string = ' '
    zero_string = '+0'
    
    # Define column mappings
    column_mapping = {
        CS_Layer_Loop_Loop_elements_FilteredFor01ProjectVoltagesSql0100Sql.LOOP_ELEMENT_CS_VOLTAGE_TYPE.value:
            Project_Voltages.VOLTAGE_TYPE.value,
        CS_Layer_Loop_Loop_elements_FilteredFor01ProjectVoltagesSql0100Sql.CS_VOLTAGE.value                  :
            Project_Voltages.VOLTAGE.value
        }
    
    # Renaming columns
    dataframe_renamed = rename_columns(
        cs_layer_loop_loop_elements_dataframe,
        column_mapping)
    
    # Filtering rows
    dataframe_filtered_non_null = filter_non_null_rows(
        dataframe_renamed,
        Project_Voltages.VOLTAGE_TYPE.value)
    dataframe_filtered_empty = filter_specific_value(
        dataframe_filtered_non_null,
        Project_Voltages.VOLTAGE_TYPE.value,
        empty_string)
    dataframe_filtered_blank = filter_specific_value(
        dataframe_filtered_empty,
        Project_Voltages.VOLTAGE_TYPE.value,
        blank_string)
    dataframe_filtered_zero = filter_specific_value(
        dataframe_filtered_blank,
        Project_Voltages.VOLTAGE.value,
        zero_string)
    
    # Selecting columns in specified order
    column_order = [Project_Voltages.VOLTAGE_TYPE.value, Project_Voltages.VOLTAGE.value]
    final_dataframe = select_columns(
        dataframe_filtered_zero,
        column_order)
    
    return final_dataframe
