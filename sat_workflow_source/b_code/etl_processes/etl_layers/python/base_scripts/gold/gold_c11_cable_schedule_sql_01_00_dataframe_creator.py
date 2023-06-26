import pandas

from sat_workflow_source.b_code.etl_schemas.common.database_names import DatabaseNames
from sat_workflow_source.b_code.etl_schemas.gold_stage.cable_schedule import Cable_Schedule
from sat_workflow_source.b_code.etl_schemas.silver_stage.s_cable_schedule import S_CableSchedule

def rename_columns(dataframe: pandas.DataFrame, renaming_dictionary: dict) -> pandas.DataFrame:
    """
    Function to rename the columns of a pandas DataFrame.
    """
    return dataframe.rename(columns=renaming_dictionary)


def filter_dataframe_on_condition(dataframe: pandas.DataFrame, condition: pandas.Series) -> pandas.DataFrame:
    """
    Function to filter a pandas DataFrame based on a condition.
    """
    return dataframe[condition]


def filter_dataframe_on_other_dataframe(dataframe1: pandas.DataFrame, dataframe2: pandas.DataFrame,
                                        column1: str, column2: str) -> pandas.DataFrame:
    """
    Function to filter a pandas DataFrame based on values in another DataFrame.
    """
    return dataframe1[dataframe1[column1].isin(dataframe2[column2])]
def create_dataframe_gold_c11_cable_schedule_sql_01_00_v0_02(
        input_tables: dict) -> pandas.DataFrame:
    cable_schedule_dataframe = \
        input_tables['Sigraph_Silver.S_CableSchedule']
    
    database_names_dataframe = \
        input_tables['VW_Database_names']
    
    # renaming the columns
    renaming_dictionary = {
        S_CableSchedule.CABLE_DRUM_NUMBER.value        : Cable_Schedule.CABLE_DRUM_NUMBER.value,
        S_CableSchedule.LAYING_CORNER_POINT.value      : Cable_Schedule.LAYING_CORNER_POINT.value,
        S_CableSchedule.EXTERNAL_DOCUMENT_OF_ITEM.value: Cable_Schedule.EXTERNAL_DOCUMENT_OF_ITEM.value,
        S_CableSchedule.INSTALLED_LENGTH.value         : Cable_Schedule.INSTALLED_LENGTH.value,
        S_CableSchedule.INSTALLATION_DATE.value        : Cable_Schedule.INSTALLATION_DATE.value,
        S_CableSchedule.ESTIMATED_LENGTH.value         : Cable_Schedule.ESTIMATED_LENGTH.value,
        S_CableSchedule.FUNCTION_TEXT_1.value          : Cable_Schedule.FUNCTION_TEXT_1.value,
        S_CableSchedule.LEVEL_OF_INSTALLATION.value    : Cable_Schedule.LEVEL_OF_INSTALLATION.value,
        S_CableSchedule.SHIELD_NUMBER.value            : Cable_Schedule.SHIELD_NUMBER.value,
        S_CableSchedule.WIRE_NUMBER.value              : Cable_Schedule.WIRE_NUMBER.value,
        S_CableSchedule.CABLE_SET.value                : Cable_Schedule.CABLE_SET.value,
        S_CableSchedule.WIRE_TYPE.value                : Cable_Schedule.WIRE_TYPE.value,
        S_CableSchedule.CONDUCTOR_TYPE.value           : Cable_Schedule.CONDUCTOR_TYPE.value,
        S_CableSchedule.INSULATING_MATERIAL.value      : Cable_Schedule.INSULATING_MATERIAL.value,
        S_CableSchedule.INDUCTANCE_PER_KM.value        : Cable_Schedule.INDUCTANCE_PER_KM.value,
        S_CableSchedule.BENDING_RADIUS.value           : Cable_Schedule.BENDING_RADIUS.value,
        S_CableSchedule.CAPACITANCE_PER_KM.value       : Cable_Schedule.CAPACITANCE_PER_KM.value,
        S_CableSchedule.SHIELD.value                   : Cable_Schedule.SHIELD.value,
        S_CableSchedule.RATED_VOLTAGE_UO.value         : Cable_Schedule.RATED_VOLTAGE_UO.value,
        S_CableSchedule.RATED_VOLTAGE_U.value          : Cable_Schedule.RATED_VOLTAGE_U.value,
        S_CableSchedule.PRECIOUS_METAL_FACTOR_2.value  : Cable_Schedule.PRECIOUS_METAL_FACTOR_2.value,
        S_CableSchedule.PRECIOUS_METAL_FACTOR_1.value  : Cable_Schedule.PRECIOUS_METAL_FACTOR_1.value,
        S_CableSchedule.SUPPLIERS_ARTICLE_NO.value     : Cable_Schedule.SUPPLIERS_ARTICLE_NUMBER.value,
        S_CableSchedule.MASS.value                     : Cable_Schedule.MASS.value,
        S_CableSchedule.COMPONENT_DESCRIPTION_1.value  : Cable_Schedule.COMPONENT_DESCRIPTION_1.value,
        S_CableSchedule.SELECTION_KEY.value            : Cable_Schedule.SELECTION_KEY.value,
        S_CableSchedule.OUTSIDE_DIAMETER.value         : Cable_Schedule.OUTSIDE_DIAMETER.value,
        S_CableSchedule.SUBASSEMBLY_INFORMATION.value  : Cable_Schedule.SUBASSEMBLY_INFORMATION.value,
        S_CableSchedule.RATED_TEMPERATURE.value        : Cable_Schedule.RATED_TEMPERATURE.value,
        S_CableSchedule.QUANTITY_UNIT.value            : Cable_Schedule.QUANTITY_UNIT.value,
        S_CableSchedule.MOUNTING_FEATURE.value         : Cable_Schedule.MOUNTING_FEATURE.value,
        S_CableSchedule.MIN_AMBIENT_TEMPERATURE.value  : Cable_Schedule.MINIMUM_AMBIENT_TEMPERATURE.value,
        S_CableSchedule.MEASURE_UNIT_QUALIFIER.value   : Cable_Schedule.MEASURE_UNIT_QUALIFIER.value,
        S_CableSchedule.MAX_AMBIENT_TEMPERATURE.value  : Cable_Schedule.MAXIMUM_AMBIENT_TEMPERATURE.value,
        S_CableSchedule.LIST_PRICE.value               : Cable_Schedule.LIST_PRICE.value,
        S_CableSchedule.EAN_NUMBER.value               : Cable_Schedule.EAN_NUMBER.value,
        S_CableSchedule.BODY_LENGTH.value              : Cable_Schedule.BODY_LENGTH.value
        }
    
    renamed_dataframe = rename_columns(
        cable_schedule_dataframe,
        renaming_dictionary)
    
    # filtering the dataframe based on the values in the other dataframe
    filtered_dataframe = filter_dataframe_on_other_dataframe(
        renamed_dataframe,
        database_names_dataframe,
        S_CableSchedule.DATABASE_NAME.value,
        DatabaseNames.DATABASE_NAME.value)

    cable_schedule_columns = [item.value for item in Cable_Schedule]

    # filter the dataframe to only include columns in the Cable_Schedule enum
    column_dataframe = filtered_dataframe[cable_schedule_columns]

    output_dataframe = column_dataframe.replace(
            {
                None: 'null'
                })
    
    # return the distinct values
    return output_dataframe.drop_duplicates()
