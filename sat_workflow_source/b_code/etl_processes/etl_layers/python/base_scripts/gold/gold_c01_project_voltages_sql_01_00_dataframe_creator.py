from sat_workflow_source.b_code.etl_schemas.gold_stage.project_voltages import Project_Voltages
from sat_workflow_source.gpt4_code.raw_python_generation.gold_stage.a_raw.a01_Project_Voltages_sql_01_00 import \
    CS_Layer_Loop_Loop_elements_FilteredForProjectVoltages


def _filter_rows(
        dataframe):
    """
    This function filters out unwanted rows based on specific conditions.
    """
    
    # changed notna in line 103 to notnull
    
    conditions = (
            dataframe[
                CS_Layer_Loop_Loop_elements_FilteredForProjectVoltages.LOOP_ELEMENT_CS_VOLTAGE_TYPE.value].notnull() &
            dataframe[
                CS_Layer_Loop_Loop_elements_FilteredForProjectVoltages.LOOP_ELEMENT_CS_VOLTAGE_TYPE.value] !=
            '_empty_' &
            dataframe[
                CS_Layer_Loop_Loop_elements_FilteredForProjectVoltages.LOOP_ELEMENT_CS_VOLTAGE_TYPE.value] != " " &
            dataframe[CS_Layer_Loop_Loop_elements_FilteredForProjectVoltages.CS_VOLTAGE.value] != '+0'
    )
    return dataframe[conditions]


def _select_columns(
        dataframe):
    """
    This function selects the desired columns and renames them if needed.
    """
    selected_columns = dataframe[
        [CS_Layer_Loop_Loop_elements_FilteredForProjectVoltages.LOOP_ELEMENT_CS_VOLTAGE_TYPE.value,
         CS_Layer_Loop_Loop_elements_FilteredForProjectVoltages.CS_VOLTAGE.value]]
    renamed_dataframe = selected_columns.rename(
            columns={
                CS_Layer_Loop_Loop_elements_FilteredForProjectVoltages.LOOP_ELEMENT_CS_VOLTAGE_TYPE.value:
                    Project_Voltages.VOLTAGE_TYPE.value,
                CS_Layer_Loop_Loop_elements_FilteredForProjectVoltages.CS_VOLTAGE.value                  :
                    Project_Voltages.VOLTAGE.value
                })
    return renamed_dataframe


def _get_distinct_rows(
        dataframe):
    """
    This function drops the duplicate rows from the dataframe.
    """
    return dataframe.drop_duplicates()


def create_dataframe_gold_c01_project_voltages_sql_01_00(
        input_tables):
    """
    This is the main function that uses the helper functions to perform the SQL operation.
    """
    table_name = 'Sigraph.CS_Layer_Loop_Loop_elements'
    initial_dataframe = input_tables[table_name]
    filtered_dataframe = _filter_rows(
        initial_dataframe)
    selected_dataframe = _select_columns(
        filtered_dataframe)
    final_dataframe = _get_distinct_rows(
        selected_dataframe)
    return final_dataframe

