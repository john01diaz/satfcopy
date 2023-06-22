import pandas as pd
from pyspark.shell import spark

from sat_workflow_source.b_code.etl_processes.common.constants import DEFAULT_CELL_VALUE
from sat_workflow_source.b_code.etl_schemas.silver_stage.s_connection import S_Connection
from sat_workflow_source.b_code.etl_schemas.silver_stage.s_item_function import S_ItemFunction
from sat_workflow_source.b_code.etl_schemas.silver_stage.s_terminals import S_Terminals
from sat_workflow_source.b_code.etl_schemas.silver_stage.s_terminations import S_Terminations


def create_silver_17_s_terminations_sql_01_00_dataframe(
        input_tables: dict):
    connection_dataframe = \
        input_tables['S_Connection']

    itemfunction_dataframe = \
        input_tables['S_ItemFunction']

    terminals_dataframe = \
        input_tables['S_Terminals']

    DEFAULT_CORE_MARKINGS = '1'
    DEFAULT_EQUIPMENT_NO = ''
    FIELD_DEVICE = 'Field Device'
    RIGHT_INDICATOR = 'R'
    LEFT_INDICATOR = 'L'
    
    # Filter dataframes before merge operations
    filtered_connection_dataframe = connection_dataframe[
        [S_Connection.DATABASE_NAME.value, S_Connection.OBJECT_IDENTIFIER.value, S_Connection.CABLE.value,
         S_Connection.WIRE_MARKINGS.value, S_Connection.FROM_LOCATION.value, S_Connection.FROM_DYNAMIC_CLASS.value,
         S_Connection.FROM_OBJECT_IDENTIFIER.value, S_Connection.FROM_TERMINAL_MARKING.value,
         S_Connection.TO_LOCATION.value, S_Connection.TO_DYNAMIC_CLASS.value,
         S_Connection.TO_OBJECT_IDENTIFIER.value, S_Connection.TO_TERMINAL_MARKING.value]]
    
    filtered_itemfunction_dataframe = itemfunction_dataframe[
        [S_ItemFunction.DATABASE_NAME.value, S_ItemFunction.DYNAMIC_CLASS.value,
         S_ItemFunction.OBJECT_IDENTIFIER.value, S_ItemFunction.TYPE.value,
         S_ItemFunction.TAG_NUMBER.value, S_ItemFunction.PRODUCT_KEY.value]]
    
    filtered_terminals_dataframe = terminals_dataframe[
        [S_Terminals.CLASS.value, S_Terminals.DATABASE_NAME.value, S_Terminals.EQUIPMENT_NO.value,
         S_Terminals.MARKING.value, S_Terminals.PARENT_EQUIPMENT_NO.value]]
    # Merge dataframes for 'from' columns
    
    first_merge = pd.merge(
            filtered_connection_dataframe,
            filtered_itemfunction_dataframe,
            left_on=[S_Connection.DATABASE_NAME.value, S_Connection.FROM_DYNAMIC_CLASS.value,
                     S_Connection.FROM_OBJECT_IDENTIFIER.value],
            right_on=[S_ItemFunction.DATABASE_NAME.value, S_ItemFunction.DYNAMIC_CLASS.value,
                      S_ItemFunction.OBJECT_IDENTIFIER.value],
            suffixes=('_S_Connection', '_S_ItemFunction'))
    first_merge[S_Terminations.PARENT_EQUIPMENT_NO.value] = first_merge[S_Connection.FROM_LOCATION.value].fillna(
            DEFAULT_EQUIPMENT_NO)
    first_merge[S_Terminations.EQUIPMENT_NO.value] = first_merge.apply(
            lambda
                row: row[S_ItemFunction.TAG_NUMBER.value] if row[S_ItemFunction.TYPE.value] == FIELD_DEVICE else row[
                S_ItemFunction.PRODUCT_KEY.value],
            axis=1)
    first_merge[S_Terminations.MARKING.value] = first_merge[S_Connection.FROM_TERMINAL_MARKING.value]
    first_merge[S_Terminations.LEFT_RIGHT.value] = RIGHT_INDICATOR
    first_result = pd.merge(
            first_merge,
            filtered_terminals_dataframe,
            left_on=[S_Terminations.PARENT_EQUIPMENT_NO.value, S_Terminations.EQUIPMENT_NO.value,
                     S_Terminations.MARKING.value],
            right_on=[S_Terminals.PARENT_EQUIPMENT_NO.value, S_Terminals.EQUIPMENT_NO.value, S_Terminals.MARKING.value],
            suffixes=('_merged', '_S_Terminals')
            ).drop_duplicates()

    # Note - added by DZa to replace None values
    first_result[S_Connection.FROM_LOCATION.value] = \
        first_result[S_Connection.FROM_LOCATION.value].fillna(
            str())

    # Note - added by DZa to replace None values
    first_result[S_Connection.TO_LOCATION.value] = \
        first_result[S_Connection.TO_LOCATION.value].fillna(
            str())

    # Filter results for 'from' and 'to' locations
    first_result = first_result[
        first_result[S_Connection.FROM_LOCATION.value] != first_result[S_Connection.TO_LOCATION.value]]
    
    # Merge dataframes for 'to' columns
    second_merge = pd.merge(
            filtered_connection_dataframe,
            filtered_itemfunction_dataframe,
            left_on=[S_Connection.DATABASE_NAME.value, S_Connection.TO_DYNAMIC_CLASS.value,
                     S_Connection.TO_OBJECT_IDENTIFIER.value],
            right_on=[S_ItemFunction.DATABASE_NAME.value, S_ItemFunction.DYNAMIC_CLASS.value,
                      S_ItemFunction.OBJECT_IDENTIFIER.value],
            suffixes=('_S_Connection', '_S_ItemFunction')
            )
    second_merge[S_Terminations.PARENT_EQUIPMENT_NO.value] = second_merge[S_Connection.TO_LOCATION.value].fillna(
            DEFAULT_EQUIPMENT_NO)
    second_merge[S_Terminations.EQUIPMENT_NO.value] = second_merge.apply(
            lambda
                row: row[S_ItemFunction.TAG_NUMBER.value] if row[S_ItemFunction.TYPE.value] == FIELD_DEVICE else row[
                S_ItemFunction.PRODUCT_KEY.value],
            axis=1)
    second_merge[S_Terminations.MARKING.value] = second_merge[S_Connection.TO_TERMINAL_MARKING.value]
    second_merge[S_Terminations.LEFT_RIGHT.value] = LEFT_INDICATOR
    second_result = pd.merge(
            second_merge,
            filtered_terminals_dataframe,
            left_on=[S_Terminations.PARENT_EQUIPMENT_NO.value, S_Terminations.EQUIPMENT_NO.value,
                     S_Terminations.MARKING.value],
            right_on=[S_Terminals.PARENT_EQUIPMENT_NO.value, S_Terminals.EQUIPMENT_NO.value, S_Terminals.MARKING.value],
            suffixes=('_merged', '_S_Terminals')
            ).drop_duplicates()

    # Note: added by DZa to replace None values
    second_result[S_Connection.FROM_LOCATION.value] = \
        second_result[S_Connection.FROM_LOCATION.value].fillna(
            str())

    second_result[S_Connection.TO_LOCATION.value] = \
        second_result[S_Connection.TO_LOCATION.value].fillna(
            str())

    # Filter results for 'from' and 'to' locations
    second_result = second_result[
        second_result[S_Connection.FROM_LOCATION.value] != second_result[S_Connection.TO_LOCATION.value]]
    
    # Union the results and sort them
    final_result = pd.concat(
            [first_result, second_result]).drop_duplicates()
    
    final_result.rename(
            columns={
                S_Connection.WIRE_MARKINGS.value: S_Terminations.CORE_MARKINGS.value,
                S_Connection.CABLE.value        : S_Terminations.CABLENUMBER.value
                },
            inplace=True)
    
    final_result.rename(
            columns=lambda
                s: s.replace(
                    "_S_Terminals",
                    ""),
            inplace=True)
    
    final_result.rename(
            columns=lambda
                s: s.replace(
                    "_merged",
                    ""),
            inplace=True)
    
    final_result.rename(
            columns=lambda
                s: s.replace(
                    "_S_Connection",
                    ""),
            inplace=True)
    
    final_result.rename(
            columns=lambda
                s: s.replace(
                    '_S_ItemFunction',
                    ""),
            inplace=True)
    
    # MANUAL ADDITION
    output_schema_columns = ['cablenumber',
                             'class',
                             'core_markings',
                             'equipment_no',
                             'left_right',
                             'marking']

 

    # Note: drop_duplicates() removed by DZa
    final_result = final_result[output_schema_columns]

    final_result[S_Terminations.EQUIPMENT_NO.value] = DEFAULT_CELL_VALUE
    final_result[S_Terminations.DATABASE_NAME.value] = DEFAULT_CELL_VALUE
    final_result[S_Terminations.OBJECT_IDENTIFIER.value] = DEFAULT_CELL_VALUE
    final_result[S_Terminations.PARENT_EQUIPMENT_NO.value] = DEFAULT_CELL_VALUE

    final_result = spark.sql(
            "select * from Terminations where database_name in (Select * from VW_Database_names)")

    return final_result
