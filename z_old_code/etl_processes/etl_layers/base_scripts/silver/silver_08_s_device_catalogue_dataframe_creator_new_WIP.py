import pandas as pd
from enum import Enum

# Define a function to extract distinct data
from sat_workflow_source.b_code.etl_schemas.common.database_names import DatabaseNames
from sat_workflow_source.b_code.etl_schemas.silver_stage.s_device_catalogue import S_DeviceCatalogue
from sat_workflow_source.b_code.etl_schemas.silver_stage.s_item_function import S_ItemFunction
from sat_workflow_source.b_code.etl_schemas.silver_stage.s_item_function_model import S_Item_Function_Model


def create_silver_08_s_device_catalogue_dataframe(s_itemfunction_dataframe, s_item_function_model_dataframe):
    # Define the columns required for the operation
    device_required_columns = [S_ItemFunction.DATABASE_NAME.value,
                               S_ItemFunction.DESCRIPTION.value,
                               S_ItemFunction.PRODUCT_KEY.value,
                               S_ItemFunction.DOCUMENT_NUMBER.value,
                               S_ItemFunction.TYPE.value,
                               S_ItemFunction.LOCATION_DESIGNATION.value,
                               S_ItemFunction.ITEM_DYNAMIC_CLASS.value,
                               S_ItemFunction.ITEM_OBJECT_IDENTIFIER.value,
                               S_ItemFunction.DYNAMIC_CLASS.value,
                               S_ItemFunction.OBJECT_IDENTIFIER.value,
                               S_ItemFunction.MANUFACTURER.value,
                               S_ItemFunction.MODELNO.value,
                               S_ItemFunction.CLASS.value,
                               S_ItemFunction.LOOP_NUMBER.value,
                               S_ItemFunction.TAG_NUMBER.value]

    model_required_columns = [S_Item_Function_Model.DATABASE_NAME.value,
                              S_Item_Function_Model.ITEM_DYNAMIC_CLASS.value,
                              S_Item_Function_Model.ITEM_OBJECT_IDENTIFIER.value,
                              S_Item_Function_Model.DYNAMIC_CLASS.value,
                              S_Item_Function_Model.OBJECT_IDENTIFIER.value,
                              S_Item_Function_Model.LEFT.value,
                              S_Item_Function_Model.RIGHT.value,
                              S_Item_Function_Model.LEFT_MARKING.value,
                              S_Item_Function_Model.RIGHT_MARKING.value,
                              S_Item_Function_Model.SYMBOL_NAME.value,
                              S_Item_Function_Model.MODELNO.value]

    # Filter dataframes with necessary columns
    device_dataframe_filtered = s_itemfunction_dataframe[device_required_columns]
    model_dataframe_filtered = s_item_function_model_dataframe[model_required_columns]

    # Merge dataframes
    merged_dataframe = pd.merge(device_dataframe_filtered, model_dataframe_filtered,
                                on=[DatabaseNames.DATABASE_NAME.value,
                                    S_Item_Function_Model.ITEM_DYNAMIC_CLASS.value,
                                    S_Item_Function_Model.ITEM_OBJECT_IDENTIFIER.value,
                                    S_Item_Function_Model.DYNAMIC_CLASS.value,
                                    S_Item_Function_Model.OBJECT_IDENTIFIER.value])

    # Compute Description
    merged_dataframe[S_DeviceCatalogue.DESCRIPTION.value] = merged_dataframe.apply(lambda row: 'RHLDD'
        if pd.isnull(row[S_ItemFunction.DESCRIPTION.value]) and pd.isnull(row[S_ItemFunction.PRODUCT_KEY.value]) and pd.isnull(row[S_ItemFunction.DOCUMENT_NUMBER.value]) and pd.isnull(row[S_ItemFunction.DATABASE_NAME.value])
        else '-'.join(filter(None, [row[S_ItemFunction.DESCRIPTION.value], row[S_ItemFunction.PRODUCT_KEY.value], row[S_ItemFunction.DOCUMENT_NUMBER.value], row[S_ItemFunction.DATABASE_NAME.value].replace('_2016R3', '')])),
        axis=1)

    # Adding new column
    merged_dataframe[S_DeviceCatalogue.ALLOW_USE.value] = 'TRUE'

    # Order by Item_Object_Identifier
    merged_dataframe = merged_dataframe.sort_values(by=S_ItemFunction.ITEM_OBJECT_IDENTIFIER.value)

    # Adding Catalogue_RNT column
    merged_dataframe[S_DeviceCatalogue.CATALOGUE_RNT.value] = merged_dataframe.groupby(S_Item_Function_Model.MODELNO.value).cumcount() + 1

    # Filtering rows
    merged_dataframe = merged_dataframe[(merged_dataframe[S_ItemFunction.TYPE.value].isin(['Device', 'FTA'])) & (merged_dataframe[S_ItemFunction.LOCATION_DESIGNATION.value].notna()) & ((merged_dataframe[S_Item_Function_Model.LEFT.value].fillna(0) + merged_dataframe[S_Item_Function_Model.RIGHT.value].fillna(0)) > 0)]

    return merged_dataframe