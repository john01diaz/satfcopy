import pandas as pd

from sat_workflow_source.b_code.etl_schemas.silver_stage.s_connection import S_Connection
from sat_workflow_source.b_code.etl_schemas.silver_stage.s_item_function import S_ItemFunction
from sat_workflow_source.b_code.etl_schemas.silver_stage.s_terminals import S_Terminals
from sat_workflow_source.b_code.etl_schemas.silver_stage.s_terminations import S_Terminations


def create_silver_17_s_terminations_sql_01_00_dataframe(s_connection_dataframe, s_item_function_dataframe, s_terminals_dataframe):
    # Inner join S_Connection, S_ItemFunction, and S_Terminals to create Terminations dataframe
    terminations_dataframe = pd.DataFrame()

    # First part of the UNION query
    part1_df = s_connection_dataframe.merge(s_item_function_dataframe, left_on=[S_Connection.DATABASE_NAME.value, S_Connection.FROM_DYNAMIC_CLASS.value,
                                                                                  S_Connection.FROM_OBJECT_IDENTIFIER.value],
                                            right_on=[S_ItemFunction.DATABASE_NAME.value, S_ItemFunction.DYNAMIC_CLASS.value, S_ItemFunction.OBJECT_IDENTIFIER.value])
    part1_df = part1_df.merge(s_terminals_dataframe, left_on=[S_Connection.FROM_LOCATION.value, S_ItemFunction.TAG_NUMBER.value],
                              right_on=[S_Terminals.PARENT_EQUIPMENT_NO.value, S_Terminals.EQUIPMENT_NO.value])
    part1_df[S_Terminations.LEFT_RIGHT.value] = 'R'
    terminations_dataframe = pd.concat([terminations_dataframe, part1_df[[S_Connection.DATABASE_NAME.value, S_Connection.OBJECT_IDENTIFIER.value,
                                                                           S_Connection.CABLE.value, S_Connection.WIRE_MARKINGS.value,
                                                                           S_Connection.FROM_LOCATION.value, S_ItemFunction.TAG_NUMBER.value,
                                                                           S_Connection.FROM_TERMINAL_MARKING.value, S_Terminations.LEFT_RIGHT.value,
                                                                           S_Terminals.CLASS.value]]])

    # Second part of the UNION query
    part2_df = s_connection_dataframe.merge(s_item_function_dataframe, left_on=[S_Connection.DATABASE_NAME.value, S_Connection.TO_DYNAMIC_CLASS.value,
                                                                                  S_Connection.TO_OBJECT_IDENTIFIER.value],
                                            right_on=[S_ItemFunction.DATABASE_NAME.value, S_ItemFunction.DYNAMIC_CLASS.value, S_ItemFunction.OBJECT_IDENTIFIER.value])
    part2_df = part2_df.merge(s_terminals_dataframe, left_on=[S_Connection.TO_LOCATION.value, S_ItemFunction.TAG_NUMBER.value],
                              right_on=[S_Terminals.PARENT_EQUIPMENT_NO.value, S_Terminals.EQUIPMENT_NO.value])
    part2_df[S_Terminations.LEFT_RIGHT.value] = 'L'
    terminations_dataframe = pd.concat([terminations_dataframe, part2_df[[S_Connection.DATABASE_NAME.value, S_Connection.OBJECT_IDENTIFIER.value,
                                                                           S_Connection.CABLE.value, S_Connection.WIRE_MARKINGS.value,
                                                                           S_Connection.TO_LOCATION.value, S_ItemFunction.TAG_NUMBER.value,
                                                                           S_Connection.TO_TERMINAL_MARKING.value, S_Terminations.LEFT_RIGHT.value,
                                                                           S_Terminals.CLASS.value]]])

    terminations_dataframe.sort_values(by=[S_Terminations.DATABASE_NAME.value, S_Terminations.OBJECT_IDENTIFIER.value], inplace=True)
    terminations_dataframe.reset_index(drop=True, inplace=True)

    # Rename columns to S_Terminations enum values
    terminations_dataframe.columns = [S_Terminations[col].value for col in terminations_dataframe.columns]

    return terminations_dataframe