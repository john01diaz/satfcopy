import pandas
import re

# Constants
PARENT_EQUIPMENT_NO_DEFAULT = ""
DYNAMIC_CLASS_CONDITION = "LC_Component_function"
VALID_MARKINGS = ["AB", "CD", "EF", "GH"]
FROM_LEFT_RIGHT_DEFAULT = "R"
TO_LEFT_RIGHT_DEFAULT = "L"

# Column names constants for s_connection
S_CONNECTION_DATABASE_NAME = "database_name"
S_CONNECTION_OBJECT_IDENTIFIER = "object_identifier"
S_CONNECTION_FROM_LOCATION = "from_location"
S_CONNECTION_FROM_ITEM = "from_item"
S_CONNECTION_CONNECTION_TYPE = "connection_type"
S_CONNECTION_FROM_TERMINAL_MARKING = "from_terminal_marking"
S_CONNECTION_FROM_DYNAMIC_CLASS = "from_dynamic_class"
S_CONNECTION_TO_LOCATION = "to_location"
S_CONNECTION_TO_ITEM = "to_item"
S_CONNECTION_TO_TERMINAL_MARKING = "to_terminal_marking"
S_CONNECTION_TO_DYNAMIC_CLASS = "to_dynamic_class"

# Column names constants for s_terminals
S_TERMINALS_PARENT_EQUIPMENT_NO = "parent_equipment_no"
S_TERMINALS_EQUIPMENT_NO = "equipment_no"
S_TERMINALS_MARKING = "marking"


def create_internal_wiring(
        dataframe_s_connection,
        dataframe_s_terminals):
    # Filtering
    filtered_s_connection = dataframe_s_connection[
        (dataframe_s_connection[S_CONNECTION_FROM_LOCATION] == dataframe_s_connection[S_CONNECTION_TO_LOCATION]) &
        dataframe_s_connection[S_CONNECTION_FROM_LOCATION].notna() &
        dataframe_s_connection[S_CONNECTION_TO_LOCATION].notna()
        ]
    
    # Merging
    dataframe_merged = pandas.merge(
        filtered_s_connection,
        dataframe_s_terminals,
        left_on=[S_CONNECTION_FROM_LOCATION, S_CONNECTION_FROM_ITEM, S_CONNECTION_FROM_TERMINAL_MARKING],
        right_on=[S_TERMINALS_PARENT_EQUIPMENT_NO, S_TERMINALS_EQUIPMENT_NO, S_TERMINALS_MARKING],
        how='inner'
        )
    dataframe_merged = pandas.merge(
        dataframe_merged,
        dataframe_s_terminals,
        left_on=[S_CONNECTION_TO_LOCATION, S_CONNECTION_TO_ITEM, S_CONNECTION_TO_TERMINAL_MARKING],
        right_on=[S_TERMINALS_PARENT_EQUIPMENT_NO, S_TERMINALS_EQUIPMENT_NO, S_TERMINALS_MARKING],
        how='inner',
        suffixes=('_DF', '_DT')
        )
    
    # Add From_Left_Right and To_Left_Right
    dataframe_merged['from_left_right'] = dataframe_merged.apply(
        lambda
            row: FROM_LEFT_RIGHT_DEFAULT if
        row[S_CONNECTION_FROM_DYNAMIC_CLASS] == DYNAMIC_CLASS_CONDITION and
        re.findall(
            '[A-Za-z]+',
            row[S_CONNECTION_FROM_TERMINAL_MARKING].upper())[0] not in VALID_MARKINGS
        else 'Normal',
        axis=1)
    dataframe_merged['to_left_right'] = dataframe_merged.apply(
        lambda
            row: TO_LEFT_RIGHT_DEFAULT if
        row[S_CONNECTION_TO_DYNAMIC_CLASS] == DYNAMIC_CLASS_CONDITION and
        re.findall(
            '[A-Za-z]+',
            row[S_CONNECTION_TO_TERMINAL_MARKING].upper())[0] not in VALID_MARKINGS
        else 'Normal',
        axis=1)
    
    # Rename the columns
    dataframe_merged = dataframe_merged.rename(
        columns={
            S_CONNECTION_DATABASE_NAME        : 'database_name',
            S_CONNECTION_OBJECT_IDENTIFIER    : 'object_identifier',
            S_CONNECTION_FROM_LOCATION        : 'from_parent_equipment_no',
            ''                                : 'from_compartment',
            S_CONNECTION_FROM_ITEM            : 'from_equipment',
            S_CONNECTION_CONNECTION_TYPE      : 'from_wire_link',
            S_CONNECTION_FROM_TERMINAL_MARKING: 'from_marking',
            S_CONNECTION_TO_LOCATION          : 'to_parent_equipment_no',
            ''                                : 'to_compartment',
            S_CONNECTION_TO_ITEM              : 'to_equipment_no',
            S_CONNECTION_CONNECTION_TYPE      : 'to_wire_link',
            S_CONNECTION_TO_TERMINAL_MARKING  : 'to_marking',
            })
    
    # Select distinct rows
    dataframe_internal_wiring = dataframe_merged.drop_duplicates()
    
    return dataframe_internal_wiring
