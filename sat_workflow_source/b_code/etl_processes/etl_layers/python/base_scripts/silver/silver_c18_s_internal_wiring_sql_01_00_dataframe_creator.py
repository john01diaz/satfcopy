import pandas
import re
from sat_workflow_source.b_code.etl_processes.common.constants import DEFAULT_CELL_VALUE
from sat_workflow_source.b_code.etl_schemas.silver_stage.s_connection import S_Connection
from sat_workflow_source.b_code.etl_schemas.silver_stage.s_internal_wiring import S_Internal_Wiring
from sat_workflow_source.b_code.etl_schemas.silver_stage.s_terminals import S_Terminals


# Function to extract letters from marking
def extract_letters(
        marking):
    letter_list = re.findall(
            '[A-Za-z]+',
            marking)
    return letter_list[0].upper() if letter_list else ''


# Function to determine the side
def determine_side(
        dynamic_class,
        marking):
    letters = extract_letters(
            marking)
    if dynamic_class == 'LC_Component_function' and letters not in ['AB', 'CD', 'EF', 'GH']:
        return 'Normal'
    else:
        return 'R' if letters == 'A' else 'L'


def create_silver_18_s_internal_wiring_sql_01_00_dataframe(
        input_tables: dict):
    s_connection_dataframe = \
        input_tables['S_Connection']

    s_terminals_dataframe = \
        input_tables['S_Terminals']

    # Preprocessing: Creating additional columns required for the analysis
    s_connection_dataframe[S_Connection.FROM_DYNAMIC_CLASS.value] = s_connection_dataframe.apply(
            lambda
                row: determine_side(
                    row[S_Connection.FROM_DYNAMIC_CLASS.value],
                    row[S_Connection.FROM_TERMINAL_MARKING.value]),
            axis=1)
    s_connection_dataframe[S_Connection.TO_DYNAMIC_CLASS.value] = s_connection_dataframe.apply(
            lambda
                row: determine_side(
                    row[S_Connection.TO_DYNAMIC_CLASS.value],
                    row[S_Connection.TO_TERMINAL_MARKING.value]),
            axis=1)
    
    # Join operations
    merged_dataframe_from = pandas.merge(
            s_connection_dataframe,
            s_terminals_dataframe,
            left_on=[S_Connection.FROM_LOCATION.value, S_Connection.FROM_ITEM.value,
                     S_Connection.FROM_TERMINAL_MARKING.value],
            right_on=[S_Terminals.PARENT_EQUIPMENT_NO.value, S_Terminals.EQUIPMENT_NO.value, S_Terminals.MARKING.value],
            how='inner',
            suffixes=('', 'term'))
    
    merged_dataframe_to = pandas.merge(
            merged_dataframe_from,
            s_terminals_dataframe,
            left_on=[S_Connection.TO_LOCATION.value, S_Connection.TO_ITEM.value,
                     S_Connection.TO_TERMINAL_MARKING.value],
            right_on=[S_Terminals.PARENT_EQUIPMENT_NO.value, S_Terminals.EQUIPMENT_NO.value, S_Terminals.MARKING.value],
            how='inner',
            suffixes=('', 'term'))
    
    # Selecting required columns and renaming them
    required_dataframe = merged_dataframe_to[
        [S_Connection.DATABASE_NAME.value, S_Connection.OBJECT_IDENTIFIER.value, S_Connection.FROM_LOCATION.value,
         S_Connection.FROM_ITEM.value,
         S_Connection.CONNECTION_TYPE.value, S_Connection.FROM_TERMINAL_MARKING.value,
         S_Connection.FROM_DYNAMIC_CLASS.value,
         S_Connection.TO_LOCATION.value, S_Connection.TO_ITEM.value, S_Connection.TO_TERMINAL_MARKING.value,
         S_Connection.TO_DYNAMIC_CLASS.value, S_Terminals.CLASS.value + 'term']].copy()
    
    required_dataframe.rename(
            columns={
                S_Terminals.CLASS.value + 'term': S_Internal_Wiring.CLASS.value
                },
            inplace=True)
    
    required_dataframe.columns = [S_Internal_Wiring.DATABASE_NAME.value, S_Internal_Wiring.OBJECT_IDENTIFIER.value,
                                  S_Internal_Wiring.FROM_PARENT_EQUIPMENT_NO.value,
                                  S_Internal_Wiring.FROM_EQUIPMENT.value, S_Internal_Wiring.FROM_WIRE_LINK.value,
                                  S_Internal_Wiring.FROM_MARKING.value,
                                  S_Internal_Wiring.FROM_LEFT_RIGHT.value,
                                  S_Internal_Wiring.TO_PARENT_EQUIPMENT_NO.value,
                                  S_Internal_Wiring.TO_EQUIPMENT_NO.value,
                                  S_Internal_Wiring.TO_MARKING.value, S_Internal_Wiring.TO_LEFT_RIGHT.value,
                                  S_Internal_Wiring.CLASS.value]
    
    # Filtering the data
    required_dataframe = required_dataframe.loc[(required_dataframe[S_Internal_Wiring.FROM_PARENT_EQUIPMENT_NO.value] ==
                                                 required_dataframe[S_Internal_Wiring.TO_PARENT_EQUIPMENT_NO.value])
                                                & (required_dataframe[
                                                       S_Internal_Wiring.FROM_PARENT_EQUIPMENT_NO.value].notna())
                                                & (required_dataframe[
                                                       S_Internal_Wiring.TO_PARENT_EQUIPMENT_NO.value].notna())]
    
    # Dropping duplicates
    required_dataframe = required_dataframe.drop_duplicates()

    # MANUAL ADDITION TO COMPLY WITH THE REQUIREMENTS OF GOLD PROCESSING

    required_dataframe.replace(
            {
                None: 'null'
                },
            inplace=True)

    required_dataframe[S_Internal_Wiring.TO_LEFT_RIGHT.value] = DEFAULT_CELL_VALUE
    required_dataframe[S_Internal_Wiring.FROM_LEFT_RIGHT.value] = DEFAULT_CELL_VALUE
    required_dataframe[S_Internal_Wiring.TO_WIRE_LINK.value] = DEFAULT_CELL_VALUE
    required_dataframe[S_Internal_Wiring.TO_COMPARTMENT.value] = DEFAULT_CELL_VALUE
    required_dataframe[S_Internal_Wiring.FROM_COMPARTMENT.value] = DEFAULT_CELL_VALUE
    required_dataframe[S_Internal_Wiring.CLASS.value] = DEFAULT_CELL_VALUE

    return required_dataframe
