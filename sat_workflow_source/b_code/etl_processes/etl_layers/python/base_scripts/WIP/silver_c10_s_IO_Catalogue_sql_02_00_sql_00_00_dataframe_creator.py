import pandas
import pandas as pd

from sat_workflow_source.b_code.etl_schemas.common.database_names import DatabaseNames
from sat_workflow_source.b_code.etl_schemas.silver_stage.s_io_catalogue import S_IO_Catalogue
from sat_workflow_source.b_code.etl_schemas.silver_stage.s_item_function import S_ItemFunction
from sat_workflow_source.b_code.etl_schemas.silver_stage.s_pin import S_Pin


def create_temp_view_vw_io_terminal_marking_1_(itemfunction_dataframe, pin_dataframe):
    merged_dataframe = pd.merge(
        itemfunction_dataframe[[DatabaseNames.DATABASE_NAME.value, S_ItemFunction.DYNAMIC_CLASS.value, S_ItemFunction.OBJECT_IDENTIFIER.value, S_ItemFunction.TYPE.value]],
        pin_dataframe[[DatabaseNames.DATABASE_NAME.value, S_Pin.FUNCTION_DYNAMIC_CLASS.value, S_Pin.FUNCTION_OBJECT_IDENTIFIER.value, S_Pin.PIN_TYPE.value, S_Pin.TERMINAL_MARKING.value]],
        left_on=[DatabaseNames.DATABASE_NAME.value, S_ItemFunction.DYNAMIC_CLASS.value, S_ItemFunction.OBJECT_IDENTIFIER.value],
        right_on=[DatabaseNames.DATABASE_NAME.value, S_Pin.FUNCTION_DYNAMIC_CLASS.value, S_Pin.FUNCTION_OBJECT_IDENTIFIER.value]
    )

    filtered_dataframe = merged_dataframe.loc[
        (merged_dataframe[S_ItemFunction.TYPE.value] == 'IO Module') &
        (merged_dataframe[S_Pin.PIN_TYPE.value] == 'EL_PIN'),
        [
            DatabaseNames.DATABASE_NAME.value,
            S_ItemFunction.DYNAMIC_CLASS.value,
            S_ItemFunction.OBJECT_IDENTIFIER.value,
            S_Pin.TERMINAL_MARKING.value
        ]
    ]

    return filtered_dataframe.rename(columns={S_Pin.TERMINAL_MARKING.value: S_IO_Catalogue.TERMINALSPERMARKING.value})

def create_temp_view_vw_io_terminal_marking_2_and_3_(itemfunction_dataframe):
    filtered_dataframe = itemfunction_dataframe.loc[
        (itemfunction_dataframe[S_ItemFunction.TYPE.value] == 'IO Module') &
        (itemfunction_dataframe[S_ItemFunction.CHANNELNUMBER.value].notnull()) &
        (itemfunction_dataframe[S_ItemFunction.CHANNELNUMBER.value] != '') &
        (itemfunction_dataframe[S_ItemFunction.CHANNELNUMBER.value] != '0'),
        [
            DatabaseNames.DATABASE_NAME.value,
            S_ItemFunction.DYNAMIC_CLASS.value,
            S_ItemFunction.OBJECT_IDENTIFIER.value,
            S_ItemFunction.CHANNELNUMBER.value,
            S_ItemFunction.TYPE.value
        ]
    ]

    filtered_dataframe = filtered_dataframe.loc[
        filtered_dataframe.apply(
            lambda row: row[S_ItemFunction.TYPE.value] != 'IO Module' or row[S_ItemFunction.CHANNELNUMBER.value] not in ('', '0'),
            axis=1
        )
    ]

    return filtered_dataframe


def silver_c10_s_IO_Catalogue_sql_02_00_sql_00_00_dataframe_creator(
        input_tables: dict) -> pandas.DataFrame:
    
    itemfunction_dataframe = \
        input_tables['S_Itemfunction']
    
    pin_dataframe = \
        input_tables['S_Pin']
    
    dataframe_1 = create_temp_view_vw_io_terminal_marking_1_(
        itemfunction_dataframe,
        pin_dataframe)
    dataframe_2 = create_temp_view_vw_io_terminal_marking_2_and_3_(
        itemfunction_dataframe)
    dataframe_3 = create_temp_view_vw_io_terminal_marking_2_and_3_(
        itemfunction_dataframe)
    
    dataframe_2[S_IO_Catalogue.TERMINALSPERMARKING.value] = dataframe_2[S_ItemFunction.CHANNELNUMBER.value] + '+'
    dataframe_3[S_IO_Catalogue.TERMINALSPERMARKING.value] = dataframe_3[S_ItemFunction.CHANNELNUMBER.value] + '-'
    
    final_dataframe = pd.concat(
            [dataframe_1, dataframe_2, dataframe_3])
    
    return final_dataframe