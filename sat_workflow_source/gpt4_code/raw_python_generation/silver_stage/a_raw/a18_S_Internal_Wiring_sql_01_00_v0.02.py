from enum import Enum
import pandas
import re

# Enum schemas
class DatabaseNames(Enum):
    DATABASE_NAME = 'database_name'

class S_Terminals(Enum):
    CLASS = 'class'
    DATABASE_NAME = 'database_name'
    DYNAMIC_CLASS = 'dynamic_class'
    EQUIPMENT_NO = 'equipment_no'
    MARKING = 'marking'
    OBJECT_IDENTIFIER = 'object_identifier'
    PARENT_EQUIPMENT_NO = 'parent_equipment_no'
    PARQUET_FILE_RELATIVE_PATH = 'parquet_file_relative_path'
    SEQUENCE = 'sequence'

class S_Connection(Enum):
    CABLE = 'cable'
    CABLE_OBJECT_IDENTIFIER = 'cable_object_identifier'
    CONNECTION_TYPE = 'connection_type'
    DATABASE_NAME = 'database_name'
    DOCUMENT_NUMBER = 'document_number'
    FROM_CONNECTION_PIN_HREF = 'from_connection_pin_href'
    FROM_DYNAMIC_CLASS = 'from_dynamic_class'
    FROM_FACILITY = 'from_facility'
    FROM_ITEM = 'from_item'
    FROM_ITEM_DYNAMIC_CLASS = 'from_item_dynamic_class'
    FROM_ITEM_OBJECT_IDENTIFIER = 'from_item_object_identifier'
    FROM_LOCATION = 'from_location'
    FROM_OBJECT_IDENTIFIER = 'from_object_identifier'
    FROM_TERMINAL_MARKING = 'from_terminal_marking'
    LOOP_NUMBER = 'loop_number'
    OBJECT_IDENTIFIER = 'object_identifier'
    PARQUET_FILE_RELATIVE_PATH = 'parquet_file_relative_path'
    TAG_NUMBER = 'tag_number'
    TO_CONNECTION_PIN_HREF = 'to_connection_pin_href'
    TO_DYNAMIC_CLASS = 'to_dynamic_class'
    TO_FACILITY = 'to_facility'
    TO_ITEM = 'to_item'
    TO_ITEM_DYNAMIC_CLASS = 'to_item_dynamic_class'
    TO_ITEM_OBJECT_IDENTIFIER = 'to_item_object_identifier'
    TO_LOCATION = 'to_location'
    TO_OBJECT_IDENTIFIER = 'to_object_identifier'
    TO_TERMINAL_MARKING = 'to_terminal_marking'
    WIRE_CROSS_SECTION = 'wire_cross_section'
    WIRE_MARKINGS = 'wire_markings'
    WIRE_OBJECT_IDENTIFIER = 'wire_object_identifier'

class S_Internal_Wiring(Enum):
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

# Function to extract letters from marking
def extract_letters(marking):
    return re.findall('[A-Za-z]+', marking)[0].upper()

# Function to determine the side
def determine_side(dynamic_class, marking):
    letters = extract_letters(marking)
    if dynamic_class == 'LC_Component_function' and letters not in ['AB','CD','EF','GH']:
        return 'Normal'
    else:
        return 'R' if letters == 'A' else 'L'

def generate_internal_wiring(dataframe_s_connection, dataframe_s_terminals):
    # Preprocessing: Creating additional columns required for the analysis
    dataframe_s_connection[S_Connection.FROM_DYNAMIC_CLASS.value] = dataframe_s_connection.apply(lambda row: determine_side(row[S_Connection.FROM_DYNAMIC_CLASS.value], row[S_Connection.FROM_TERMINAL_MARKING.value]), axis=1)
    dataframe_s_connection[S_Connection.TO_DYNAMIC_CLASS.value] = dataframe_s_connection.apply(lambda row: determine_side(row[S_Connection.TO_DYNAMIC_CLASS.value], row[S_Connection.TO_TERMINAL_MARKING.value]), axis=1)

    # Join operations
    merged_dataframe_from = pandas.merge(dataframe_s_connection, dataframe_s_terminals,
                        left_on=[S_Connection.FROM_LOCATION.value, S_Connection.FROM_ITEM.value, S_Connection.FROM_TERMINAL_MARKING.value],
                        right_on=[S_Terminals.PARENT_EQUIPMENT_NO.value, S_Terminals.EQUIPMENT_NO.value, S_Terminals.MARKING.value],
                        how='inner')

    merged_dataframe_to = pandas.merge(merged_dataframe_from, dataframe_s_terminals,
                        left_on=[S_Connection.TO_LOCATION.value, S_Connection.TO_ITEM.value, S_Connection.TO_TERMINAL_MARKING.value],
                        right_on=[S_Terminals.PARENT_EQUIPMENT_NO.value, S_Terminals.EQUIPMENT_NO.value, S_Terminals.MARKING.value],
                        how='inner')

    # Selecting required columns and renaming them
    required_dataframe = merged_dataframe_to[[S_Connection.DATABASE_NAME.value, S_Connection.OBJECT_IDENTIFIER.value, S_Connection.FROM_LOCATION.value, S_Connection.FROM_ITEM.value,
                                S_Connection.CONNECTION_TYPE.value, S_Connection.FROM_TERMINAL_MARKING.value, S_Connection.FROM_DYNAMIC_CLASS.value,
                                S_Connection.TO_LOCATION.value, S_Connection.TO_ITEM.value, S_Connection.TO_TERMINAL_MARKING.value,
                                S_Connection.TO_DYNAMIC_CLASS.value, S_Terminals.CLASS.value]].copy()

    required_dataframe.columns = [S_Internal_Wiring.DATABASE_NAME.value, S_Internal_Wiring.OBJECT_IDENTIFIER.value, S_Internal_Wiring.FROM_PARENT_EQUIPMENT_NO.value,
                                S_Internal_Wiring.FROM_EQUIPMENT.value, S_Internal_Wiring.FROM_WIRE_LINK.value, S_Internal_Wiring.FROM_MARKING.value,
                                S_Internal_Wiring.FROM_LEFT_RIGHT.value, S_Internal_Wiring.TO_PARENT_EQUIPMENT_NO.value, S_Internal_Wiring.TO_EQUIPMENT_NO.value,
                                S_Internal_Wiring.TO_MARKING.value, S_Internal_Wiring.TO_LEFT_RIGHT.value, S_Internal_Wiring.CLASS.value]

    # Filtering the data
    required_dataframe = required_dataframe.loc[(required_dataframe[S_Internal_Wiring.FROM_PARENT_EQUIPMENT_NO.value] == required_dataframe[S_Internal_Wiring.TO_PARENT_EQUIPMENT_NO.value])
                                                & (required_dataframe[S_Internal_Wiring.FROM_PARENT_EQUIPMENT_NO.value].notna())
                                                & (required_dataframe[S_Internal_Wiring.TO_PARENT_EQUIPMENT_NO.value].notna())]

    # Dropping duplicates
    required_dataframe = required_dataframe.drop_duplicates()

    return required_dataframe