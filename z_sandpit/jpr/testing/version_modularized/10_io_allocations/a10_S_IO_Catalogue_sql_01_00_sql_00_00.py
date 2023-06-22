import pandas as pd

from sat_workflow_source.b_code.etl_schemas.silver_stage.s_io_catalogue import S_IO_Catalogue
from sat_workflow_source.b_code.etl_schemas.silver_stage.s_item_function import S_ItemFunction
from sat_workflow_source.b_code.etl_schemas.silver_stage.s_item_function_model import S_Item_Function_Model


def process_dataframes(
        dataframe_one,
        dataframe_two):
    # Step 1: Prepare and join the dataframes
    merged_dataframe = prepare_join(
        dataframe_one,
        dataframe_two)
    
    # Step 2: Obtain distinct rows
    distinct_dataframe = get_distinct(
        merged_dataframe)
    
    # Step 3: Apply window functions
    windowed_dataframe = apply_window_functions(
        distinct_dataframe)
    
    # Step 4: Rename columns
    final_dataframe = rename_columns(
        windowed_dataframe)
    
    return final_dataframe


def prepare_join(
        dataframe_one,
        dataframe_two):
    # Merging dataframes
    merged_dataframe = pd.merge(
            dataframe_one,
            dataframe_two,
            how='inner',
            left_on=[
                S_ItemFunction.DATABASE_NAME.value,
                S_ItemFunction.ITEM_DYNAMIC_CLASS.value,
                S_ItemFunction.ITEM_OBJECT_IDENTIFIER.value,
                S_ItemFunction.DYNAMIC_CLASS.value,
                S_ItemFunction.OBJECT_IDENTIFIER.value,
                ],
            right_on=[
                S_Item_Function_Model.DATABASE_NAME.value,
                S_Item_Function_Model.ITEM_DYNAMIC_CLASS.value,
                S_Item_Function_Model.ITEM_OBJECT_IDENTIFIER.value,
                S_Item_Function_Model.DYNAMIC_CLASS.value,
                S_Item_Function_Model.OBJECT_IDENTIFIER.value,
                ],
            )
    
    # Applying filters
    filtered_dataframe = merged_dataframe[
        (merged_dataframe[S_Item_Function_Model.TYPE.value] == 'IO Module') &
        (merged_dataframe[S_ItemFunction.CHANNELNUMBER.value].notnull()) &
        (merged_dataframe[S_ItemFunction.CHANNELNUMBER.value] != '0') &
        (merged_dataframe[S_ItemFunction.CHANNELNUMBER.value] != '')
        ]
    
    return filtered_dataframe



def get_distinct(
        dataframe):
    distinct_dataframe = dataframe.drop_duplicates()
    return distinct_dataframe


def apply_window_functions(
        dataframe):
    dataframe[S_IO_Catalogue.DESCRIPTION.value] = dataframe.groupby(
            [
                S_ItemFunction.DATABASE_NAME.value,
                S_ItemFunction.IOTYPE.value,
                S_ItemFunction.ITEM_OBJECT_IDENTIFIER.value
                ])[S_ItemFunction.CHANNELNUMBER.value].transform(
        'count') + ' - Channel -' + dataframe[S_Item_Function_Model.MANUFACTURER.value]
    
    dataframe[S_IO_Catalogue.MANUFACTURER.value] = dataframe.groupby(
            S_Item_Function_Model.MODELNO.value)[S_Item_Function_Model.MANUFACTURER.value].transform(
        'max')
    
    dataframe[S_IO_Catalogue.NOOFPOINTS.value] = dataframe.groupby(
            [
                S_ItemFunction.DATABASE_NAME.value,
                S_ItemFunction.IOTYPE.value,
                S_ItemFunction.ITEM_OBJECT_IDENTIFIER.value
                ])[S_ItemFunction.CHANNELNUMBER.value].transform(
        'count')
    
    return dataframe



def rename_columns(
        dataframe):
    dataframe.rename(
            columns={
                S_ItemFunction.DATABASE_NAME.value: S_IO_Catalogue.DATABASE_NAME.value,
                S_ItemFunction.DYNAMIC_CLASS.value: S_IO_Catalogue.DYNAMIC_CLASS.value,
                S_ItemFunction.OBJECT_IDENTIFIER.value: S_IO_Catalogue.OBJECT_IDENTIFIER.value,
                S_Item_Function_Model.MODELNO.value: S_IO_Catalogue.MODEL_NUMBER.value,
                S_ItemFunction.IOTYPE.value: S_IO_Catalogue.IOTYPE.value,
                S_ItemFunction.CLASS.value: S_IO_Catalogue.CLASS.value,
                S_ItemFunction.CHANNELNUMBER.value: S_IO_Catalogue.CHANNEL.value
                },
            inplace=True
            )
    return dataframe