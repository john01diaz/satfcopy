import pandas as pd

from sat_workflow_source.b_code.etl_schemas.silver_stage.s_connection import S_Connection
from sat_workflow_source.b_code.etl_schemas.silver_stage.s_item_function import S_ItemFunction
from sat_workflow_source.b_code.etl_schemas.silver_stage.s_terminals import S_Terminals
from sat_workflow_source.b_code.etl_schemas.silver_stage.s_terminations import S_Terminations


def create_silver_17_s_terminations_sql_01_00_dataframe(s_connection_dataframe, s_item_function_dataframe, s_terminals_dataframe):
    # Filter the required columns from S_Connection dataframe
    s_connection_filtered = s_connection_dataframe[[S_Connection.DATABASE_NAME.value, S_Connection.OBJECT_IDENTIFIER.value,
                                                    S_Connection.CABLE.value, S_Connection.WIRE_MARKINGS.value,
                                                    S_Connection.FROM_LOCATION.value, S_Connection.FROM_TERMINAL_MARKING.value,
                                                    S_Connection.TO_LOCATION.value, S_Connection.TO_TERMINAL_MARKING.value]]

    # Filter the required columns from S_ItemFunction dataframe
    s_item_function_filtered = s_item_function_dataframe[[S_ItemFunction.DATABASE_NAME.value, S_ItemFunction.DYNAMIC_CLASS.value,
                                                          S_ItemFunction.OBJECT_IDENTIFIER.value, S_ItemFunction.ITEM_OBJECT_IDENTIFIER.value]]

    # Filter the required columns from S_Terminals dataframe
    s_terminals_filtered = s_terminals_dataframe[[S_Terminals.PARENT_EQUIPMENT_NO.value, S_Terminals.EQUIPMENT_NO.value,
                                                  S_Terminals.CLASS.value]]

    # Join S_Connection with S_ItemFunction and S_Terminals
    join_df = s_connection_filtered.merge(s_item_function_filtered,
                                           left_on=[S_Connection.DATABASE_NAME.value, S_Connection.FROM_DYNAMIC_CLASS.value, S_Connection.FROM_OBJECT_IDENTIFIER.value],
                                           right_on=[S_ItemFunction.DATABASE_NAME.value, S_ItemFunction.DYNAMIC_CLASS.value, S_ItemFunction.OBJECT_IDENTIFIER.value],
                                           suffixes=('_s_connection', '_s_item_function'))
    join_df = join_df.merge(s_terminals_filtered,
                            left_on=[S_Connection.FROM_LOCATION.value, S_ItemFunction.ITEM_OBJECT_IDENTIFIER.value],
                            right_on=[S_Terminals.PARENT_EQUIPMENT_NO.value, S_Terminals.EQUIPMENT_NO.value],
                            suffixes=('_s_connection', '_s_terminals'))
    join_df[S_Terminations.LEFT_RIGHT.value] = 'R'

    # Select the required columns for part1
    part1_df = join_df[[S_Connection.DATABASE_NAME.value, S_Connection.OBJECT_IDENTIFIER.value, S_Connection.CABLE.value,
                        S_Connection.WIRE_MARKINGS.value, S_Connection.FROM_LOCATION.value, S_ItemFunction.ITEM_OBJECT_IDENTIFIER.value,
                        S_Connection.FROM_TERMINAL_MARKING.value, S_Terminals.CLASS.value, S_ItemFunction.TAG_NUMBER.value]]

    # Join S_Connection with S_ItemFunction and S_Terminals again for part2
    join_df = s_connection_filtered.merge(s_item_function_filtered,
                                           left_on=[S_Connection.DATABASE_NAME.value, S_Connection.TO_DYNAMIC_CLASS.value, S_Connection.TO_OBJECT_IDENTIFIER.value],
                                           right_on=[S_ItemFunction.DATABASE_NAME.value, S_ItemFunction.DYNAMIC_CLASS.value, S_ItemFunction.OBJECT_IDENTIFIER.value],
                                           suffixes=('_s_connection', '_s_item_function'))
    join_df = join_df.merge(s_terminals_filtered,
                            left_on=[S_Connection.TO_LOCATION.value, S_ItemFunction.ITEM_OBJECT_IDENTIFIER.value],
                            right_on=[S_Terminals.PARENT_EQUIPMENT_NO.value, S_Terminals.EQUIPMENT_NO.value],
                            suffixes=('_s_connection', '_s_terminals'))
    join_df[S_Terminations.LEFT_RIGHT.value] = 'L'

    # Select the required columns for part2
    part2_df = join_df[[S_Connection.DATABASE_NAME.value, S_Connection.OBJECT_IDENTIFIER.value, S_Connection.CABLE.value,
                        S_Connection.WIRE_MARKINGS.value, S_Connection.TO_LOCATION.value, S_ItemFunction.ITEM_OBJECT_IDENTIFIER.value,
                        S_Connection.TO_TERMINAL_MARKING.value, S_Terminals.CLASS.value, S_ItemFunction.TAG_NUMBER.value]]

    # Concatenate part1 and part2 dataframes
    terminations_dataframe = pd.concat([part1_df, part2_df])

    # Sort and reset index
    terminations_dataframe.sort_values(by=[S_Connection.DATABASE_NAME.value, S_Connection.OBJECT_IDENTIFIER.value], inplace=True)
    terminations_dataframe.reset_index(drop=True, inplace=True)

    # Rename columns to S_Terminations enum values
    terminations_dataframe.columns = [S_Terminations[col].value if col in S_Terminations.__members__ else col for col in terminations_dataframe.columns]

    return terminations_dataframe
