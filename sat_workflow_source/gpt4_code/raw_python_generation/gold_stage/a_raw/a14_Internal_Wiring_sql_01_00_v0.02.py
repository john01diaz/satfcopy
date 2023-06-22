
from enum import Enum
import pandas


class Internal_Wiring(
        Enum):
    FROM_PARENT_EQUIPMENT_NO = 'from_parent_equipment_no'
    FROM_COMPARTMENT = 'from_compartment'
    FROM_EQUIPMENT = 'from_equipment'
    FROM_WIRE_LINK = 'from_wire_link'
    FROM_MARKING = 'from_marking'
    FROM_LEFT_RIGHT = 'from_left_right'
    TO_PARENT_EQUIPMENT_NO = 'to_parent_equipment_no'
    TO_COMPARTMENT = 'to_compartment'
    TO_EQUIPMENT_NO = 'to_equipment_no'
    TO_WIRE_LINK = 'to_wire_link'
    TO_MARKING = 'to_marking'
    TO_LEFT_RIGHT = 'to_left_right'


class S_Internal_Wiring(
        Enum):
    CLASS = 'class'
    DATABASE_NAME = 'database_name'
    FROM_COMPARTMENT = 'from_compartment'
    FROM_EQUIPMENT = 'from_equipment'
    FROM_LEFT_RIGHT = 'from_left_right'
    FROM_MARKING = 'from_marking'
    FROM_PARENT_EQUIPMENT_NO = 'from_parent_equipment_no'
    FROM_WIRE_LINK = 'from_wire_link'
    OBJECT_IDENTIFIER = 'object_identifier'
    PARQUET_FILE_RELATIVE_PATH = 'parquet_file_relative_path'
    TO_COMPARTMENT = 'to_compartment'
    TO_EQUIPMENT_NO = 'to_equipment_no'
    TO_LEFT_RIGHT = 'to_left_right'
    TO_MARKING = 'to_marking'
    TO_PARENT_EQUIPMENT_NO = 'to_parent_equipment_no'
    TO_WIRE_LINK = 'to_wire_link'


def extract_internal_wiring(
        s_internal_wiring_dataframe,
        database_names_dataframe):
    classes = ['Instrumentation', 'Inst(Shared)', 'Elec(Shared)']
    database_names = database_names_dataframe['Database_name'].unique()
    
    filtered_dataframe = s_internal_wiring_dataframe[
        (s_internal_wiring_dataframe[S_Internal_Wiring.CLASS.value].isin(
            classes)) &
        (s_internal_wiring_dataframe[S_Internal_Wiring.DATABASE_NAME.value].isin(
            database_names))
        ]
    
    columns_to_select = [
        S_Internal_Wiring.FROM_PARENT_EQUIPMENT_NO.value,
        S_Internal_Wiring.FROM_COMPARTMENT.value,
        S_Internal_Wiring.FROM_EQUIPMENT.value,
        S_Internal_Wiring.FROM_WIRE_LINK.value,
        S_Internal_Wiring.FROM_MARKING.value,
        S_Internal_Wiring.FROM_LEFT_RIGHT.value,
        S_Internal_Wiring.TO_PARENT_EQUIPMENT_NO.value,
        S_Internal_Wiring.TO_COMPARTMENT.value,
        S_Internal_Wiring.TO_EQUIPMENT_NO.value,
        S_Internal_Wiring.TO_WIRE_LINK.value,
        S_Internal_Wiring.TO_MARKING.value,
        S_Internal_Wiring.TO_LEFT_RIGHT.value
        ]
    
    internal_wiring_dataframe = filtered_dataframe[columns_to_select].drop_duplicates()
    
    return internal_wiring_dataframe

