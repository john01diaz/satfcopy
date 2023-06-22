from enum import Enum
import pandas
import re


# Function to extract letters from marking
def extract_letters(
        marking):
    return re.findall(
        '[A-Za-z]+',
        marking)[0].upper()


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


def generate_internal_wiring(
        dataframe_s_connection,
        dataframe_s_terminals):
    # Preprocessing: Creating additional columns required for the analysis
    dataframe_s_connection[S_Connection.FROM_DYNAMIC_CLASS.value] = dataframe_s_connection.apply(
        lambda
            row: determine_side(
                row[S_Connection.FROM_DYNAMIC_CLASS.value],
                row[S_Connection.FROM_TERMINAL_MARKING.value]),
        axis=1)
    dataframe_s_connection[S_Connection.TO_DYNAMIC_CLASS.value] = dataframe_s_connection.apply(
        lambda
            row: determine_side(
                row[S_Connection.TO_DYNAMIC_CLASS.value],
                row[S_Connection.TO_TERMINAL_MARKING.value]),
        axis=1)
    
    # Join operations
    merged_dataframe_from = pandas.merge(
        dataframe_s_connection,
        dataframe_s_terminals,
        left_on=[S_Connection.FROM_LOCATION.value, S_Connection.FROM_ITEM.value,
                 S_Connection.FROM_TERMINAL_MARKING.value],
        right_on=[S_Terminals.PARENT_EQUIPMENT_NO.value, S_Terminals.EQUIPMENT_NO.value, S_Terminals.MARKING.value],
        how='inner')
    
    merged_dataframe_to = pandas.merge(
        merged_dataframe_from,
        dataframe_s_terminals,
        left_on=[S_Connection.TO_LOCATION.value, S_Connection.TO_ITEM.value, S_Connection.TO_TERMINAL_MARKING.value],
        right_on=[S_Terminals.PARENT_EQUIPMENT_NO.value, S_Terminals.EQUIPMENT_NO.value, S_Terminals.MARKING.value],
        how='inner')
    
    # Selecting required columns and renaming them
    required_dataframe = merged_dataframe_to[
        [S_Connection.DATABASE_NAME.value, S_Connection.OBJECT_IDENTIFIER.value, S_Connection.FROM_LOCATION.value,
         S_Connection.FROM_ITEM.value,
         S_Connection.CONNECTION_TYPE.value, S_Connection.FROM_TERMINAL_MARKING.value,
         S_Connection.FROM_DYNAMIC_CLASS.value,
         S_Connection.TO_LOCATION.value, S_Connection.TO_ITEM.value, S_Connection.TO_TERMINAL_MARKING.value,
         S_Connection.TO_DYNAMIC_CLASS.value, S_Terminals.CLASS.value]].copy()
    
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
    
    return required_dataframe
