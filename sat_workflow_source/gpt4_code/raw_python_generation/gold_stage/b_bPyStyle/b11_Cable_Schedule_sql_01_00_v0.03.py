from typing import List
import pandas as pd

# Define enum classes for maintaining column names
from enum import Enum


class DatabaseNames(
        Enum):
    DATABASE_NAME = 'database_name'


class Cable_Schedule(
        Enum):


# all the columns as per the enum you provided

class S_CableSchedule(
        Enum):


# all the columns as per the enum you provided


# Define a function that performs the SQL equivalent operation using pandas
def filter_and_rename_columns(
        s_cable_schedule_dataframe: pd.DataFrame,
        vw_database_names_dataframe: pd.DataFrame) -> pd.DataFrame:
    
    # Create a dictionary for column renaming
    rename_dict = {
        S_CableSchedule.CABLE_DRUM_NUMBER.value        : "Cable Drum Number",
        S_CableSchedule.LAYING_CORNER_POINT.value      : "Laying Corner Point",
        S_CableSchedule.EXTERNAL_DOCUMENT_OF_ITEM.value: "External document of item",
        S_CableSchedule.INSTALLED_LENGTH.value         : "Installed Length",
        S_CableSchedule.INSTALLATION_DATE.value        : "Installation Date",
        S_CableSchedule.ESTIMATED_LENGTH.value         : "Estimated Length",
        S_CableSchedule.FUNCTION_TEXT_1.value          : "Function text 1",
        S_CableSchedule.LEVEL_OF_INSTALLATION.value    : "Level of Installation",
        S_CableSchedule.SHIELD_NUMBER.value            : "Shield number",
        S_CableSchedule.WIRE_NUMBER.value              : "Wire number",
        S_CableSchedule.CABLE_SET.value                : "Cable Set",
        S_CableSchedule.WIRE_TYPE.value                : "Wire type",
        S_CableSchedule.CONDUCTOR_TYPE.value           : "Conductor type",
        S_CableSchedule.INSULATING_MATERIAL.value      : "Insulating material",
        S_CableSchedule.INDUCTANCE_PER_KM.value        : "Inductance Per km",
        S_CableSchedule.BENDING_RADIUS.value           : "Bending radius",
        S_CableSchedule.CAPACITANCE_PER_KM.value       : "Capacitance Per km",
        S_CableSchedule.SHIELD.value                   : "Shield",
        S_CableSchedule.RATED_VOLTAGE_UO.value         : "Rated_voltage Uo",
        S_CableSchedule.RATED_VOLTAGE_U.value          : "Rated_voltage U",
        S_CableSchedule.PRECIOUS_METAL_FACTOR_2.value  : "Precious metal factor 2",
        S_CableSchedule.PRECIOUS_METAL_FACTOR_1.value  : "Precious metal factor 1",
        S_CableSchedule.SUPPLIERS_ARTICLE_NO.value     : "Suppliers article number",
        S_CableSchedule.MASS.value                     : "Mass",
        S_CableSchedule.COMPONENT_DESCRIPTION_1.value  : "Component description 1",
        S_CableSchedule.SELECTION_KEY.value            : "Selection key",
        S_CableSchedule.OUTSIDE_DIAMETER.value         : "Outside diameter",
        S_CableSchedule.SUBASSEMBLY_INFORMATION.value  : "Subassembly information",
        S_CableSchedule.RATED_TEMPERATURE.value        : "Rated temperature",
        S_CableSchedule.QUANTITY_UNIT.value            : "Quantity unit",
        S_CableSchedule.MOUNTING_FEATURE.value         : "Mounting feature",
        S_CableSchedule.MIN_AMBIENT_TEMPERATURE.value  : "minimum ambient temperature",
        S_CableSchedule.MEASURE_UNIT_QUALIFIER.value   : "Measure unit qualifier",
        S_CableSchedule.MAX_AMBIENT_TEMPERATURE.value  : "maximum ambient temperature",
        S_CableSchedule.LIST_PRICE.value               : "List price",
        S_CableSchedule.EAN_NUMBER.value               : "EAN number",
        S_CableSchedule.BODY_LENGTH.value              : "Body length"
        }
    
    # Filter the rows where database_name is in vw_database_names_dataframe
    filtered_dataframe = s_cable_schedule_dataframe[
        s_cable_schedule_dataframe[S_CableSchedule.DATABASE_NAME.value].isin(
                vw_database_names_dataframe[DatabaseNames.DATABASE_NAME.value])
    ]
    
    # Drop duplicates
    distinct_dataframe = filtered_dataframe.drop_duplicates()
    
    # Rename the columns as per the rename_dict
    renamed_dataframe = distinct_dataframe.rename(
            columns=rename_dict)
    
    return renamed_dataframe


# Call the function with your input dataframes
result_dataframe = filter_and_rename_columns(
        s_cable_schedule_dataframe,
        vw_database_names_dataframe)
