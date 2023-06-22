import pandas
import pandas as pd

from sat_workflow_source.b_code.etl_schemas.common.database_names import DatabaseNames
from sat_workflow_source.b_code.etl_schemas.silver_stage.s_io_catalogue import S_IO_Catalogue
from sat_workflow_source.b_code.etl_schemas.silver_stage.s_item_function import S_ItemFunction
from sat_workflow_source.b_code.etl_schemas.silver_stage.s_item_function_model import S_Item_Function_Model

def create_view_vw_io_terminal_marking_1_(itemfunction_dataframe, pin_dataframe):
    merged_dataframe = pandas.merge(itemfunction_dataframe, pin_dataframe,
                                    left_on=[DatabaseNames.DATABASE_NAME.value,
                                              S_IO_Catalogue.DYNAMIC_CLASS.value,
                                              S_IO_Catalogue.OBJECT_IDENTIFIER.value],
                                    right_on=[DatabaseNames.DATABASE_NAME.value,
                                              S_IO_Catalogue.DYNAMIC_CLASS.value,
                                              S_IO_Catalogue.OBJECT_IDENTIFIER.value],
                                    suffixes=('', '_y'))

    merged_dataframe = merged_dataframe[merged_dataframe.Type=='IO Module']
    merged_dataframe = merged_dataframe[merged_dataframe.Pin_Type=='EL_PIN']

    return merged_dataframe
def create_view_vw_io_terminal_marking_2_and_3_(itemfunction_dataframe):
    itemfunction_dataframe = itemfunction_dataframe[itemfunction_dataframe.Type=='IO Module']
    itemfunction_dataframe = itemfunction_dataframe[
                                (itemfunction_dataframe.ChannelNumber.notnull())
                                & (itemfunction_dataframe.ChannelNumber!='')
                                & (itemfunction_dataframe.ChannelNumber!='0')
                            ]
    itemfunction_dataframe['Condition'] = itemfunction_dataframe.apply(
        lambda row: 0 if row['Type']=='IO Module' and
                      (row['ChannelNumber']=='' or row['ChannelNumber']=='0') else 1, axis=1)

    return itemfunction_dataframe[itemfunction_dataframe.Condition==1]

def create_final_dataframe_(itemfunction_dataframe, pin_dataframe):
    dataframe_1 = create_view_vw_io_terminal_marking_1_(itemfunction_dataframe, pin_dataframe)
    dataframe_2 = create_view_vw_io_terminal_marking_2_and_3_(itemfunction_dataframe)
    dataframe_3 = create_view_vw_io_terminal_marking_2_and_3_(itemfunction_dataframe)

    dataframe_1[S_IO_Catalogue.TERMINALSPERMARKING.value] = dataframe_1['Terminal_Marking']
    dataframe_2[S_IO_Catalogue.TERMINALSPERMARKING.value] = dataframe_2['ChannelNumber'].map(str) + '+'
    dataframe_3[S_IO_Catalogue.TERMINALSPERMARKING.value] = dataframe_3['ChannelNumber'].map(str) + '-'

    final_dataframe = pandas.concat([dataframe_1, dataframe_2, dataframe_3])

    return final_dataframe
