import pandas as pd

from sat_workflow_source.b_code.etl_processes.common.constants import DEFAULT_CELL_VALUE
from sat_workflow_source.b_code.etl_schemas.silver_stage.s_device_catalogue import S_DeviceCatalogue
from sat_workflow_source.b_code.etl_schemas.silver_stage.s_item_function import S_ItemFunction
from sat_workflow_source.b_code.etl_schemas.silver_stage.s_item_function_model import S_Item_Function_Model


def create_silver_08_s_device_catalogue_dataframe(
        input_tables: dict):
    s_itemfunction_dataframe = \
        input_tables['Sigraph_Silver.S_ItemFunction']

    s_item_function_model_dataframe = \
        input_tables['Sigraph_Silver.S_Item_Function_Model']

    # Select required columns
    s_itemfunction_dataframe = s_itemfunction_dataframe[[S_ItemFunction.DATABASE_NAME.value,
                                                         S_ItemFunction.DEVICE_TYPE.value,
                                                         S_ItemFunction.DESCRIPTION.value,
                                                         S_ItemFunction.PRODUCT_KEY.value,
                                                         S_ItemFunction.DOCUMENT_NUMBER.value,
                                                         S_ItemFunction.ITEM_OBJECT_IDENTIFIER.value,
                                                         S_ItemFunction.ITEM_DYNAMIC_CLASS.value,
                                                         S_ItemFunction.DYNAMIC_CLASS.value,
                                                         S_ItemFunction.OBJECT_IDENTIFIER.value,
                                                         S_ItemFunction.TYPE.value,
                                                         S_ItemFunction.LOCATION_DESIGNATION.value,
                                                         S_ItemFunction.MANUFACTURER.value,
                                                         S_ItemFunction.CLASS.value,
                                                         # TODO manually commented out the 2 lines below
                                                         # S_ItemFunction.LEFT_MARKING.value,
                                                         # S_ItemFunction.RIGHT_MARKING.value,
                                                         # S_ItemFunction.SYMBOL_NAME.value,
                                                         S_ItemFunction.LOOP_NUMBER.value,
                                                         S_ItemFunction.TAG_NUMBER.value]]

    s_item_function_model_dataframe = s_item_function_model_dataframe[[S_Item_Function_Model.DATABASE_NAME.value,
                                                                       S_Item_Function_Model.MODELNO.value,
                                                                       S_Item_Function_Model.ITEM_DYNAMIC_CLASS.value,
                                                                       S_Item_Function_Model.ITEM_OBJECT_IDENTIFIER.value,
                                                                       S_Item_Function_Model.DYNAMIC_CLASS.value,
                                                                       S_Item_Function_Model.LEFT.value,
                                                                       S_Item_Function_Model.RIGHT.value,
                                                                       S_Item_Function_Model.LEFT_MARKING.value,
                                                                       S_Item_Function_Model.RIGHT_MARKING.value,
                                                                       S_Item_Function_Model.SYMBOL_NAME.value,
                                                                       # TODO manually added the line below
                                                                       S_Item_Function_Model.OBJECT_IDENTIFIER.value,
                                                                       # TODO manually commented the lines below
                                                                       # S_Item_Function_Model.LOOP_NUMBER.value,
                                                                       # S_Item_Function_Model.TAG_NUMBER.value,
                                                                       # S_Item_Function_Model.DOCUMENT_NUMBER.value
                                                                       ]]

    # Step 1: Join the dataframes
    merged_dataframe = pd.merge(
        s_itemfunction_dataframe,
        s_item_function_model_dataframe,
        how='inner',
        left_on=[S_ItemFunction.DATABASE_NAME.value,
                 S_ItemFunction.ITEM_DYNAMIC_CLASS.value,
                 S_ItemFunction.ITEM_OBJECT_IDENTIFIER.value,
                 S_ItemFunction.DYNAMIC_CLASS.value,
                 S_ItemFunction.OBJECT_IDENTIFIER.value],
        right_on=[S_Item_Function_Model.DATABASE_NAME.value,
                  S_Item_Function_Model.ITEM_DYNAMIC_CLASS.value,
                  S_Item_Function_Model.ITEM_OBJECT_IDENTIFIER.value,
                  S_Item_Function_Model.DYNAMIC_CLASS.value,
                  S_Item_Function_Model.OBJECT_IDENTIFIER.value]
    )

    # Step 2: Filter rows based on several conditions
    merged_dataframe = merged_dataframe[
        merged_dataframe[S_ItemFunction.TYPE.value].isin(['Device', 'FTA']) &
        merged_dataframe[S_ItemFunction.LOCATION_DESIGNATION.value].notna() &
        ((merged_dataframe[S_Item_Function_Model.LEFT.value].fillna(0) + merged_dataframe[
            S_Item_Function_Model.RIGHT.value].fillna(0)) > 0)
        ]

    # Step 3: Create new derived columns and perform transformations
    merged_dataframe[S_DeviceCatalogue.ALLOW_USE.value] = "TRUE"
    merged_dataframe[S_DeviceCatalogue.MODEL_NO.value] = merged_dataframe[S_Item_Function_Model.MODELNO.value]
    merged_dataframe[S_DeviceCatalogue.DESCRIPTION.value] = merged_dataframe.apply(create_description, axis=1)
    merged_dataframe[S_DeviceCatalogue.MANUFACTURER.value] = merged_dataframe[S_ItemFunction.MANUFACTURER.value]
    merged_dataframe[S_DeviceCatalogue.CLASS.value] = merged_dataframe[S_ItemFunction.CLASS.value]
    # TODO commented out the rows below
    # merged_dataframe[S_DeviceCatalogue.LEFT_MARKING.value] = merged_dataframe[S_ItemFunction.LEFT_MARKING.value]
    # merged_dataframe[S_DeviceCatalogue.RIGHT_MARKING.value] = merged_dataframe[S_ItemFunction.RIGHT_MARKING.value]
    # merged_dataframe[S_DeviceCatalogue.SYMBOL_NAME.value] = merged_dataframe[S_ItemFunction.SYMBOL_NAME.value]
    # merged_dataframe[S_DeviceCatalogue.LOOP_NUMBER.value] = merged_dataframe[S_ItemFunction.LOOP_NUMBER.value]
    # merged_dataframe[S_DeviceCatalogue.TAG_NUMBER.value] = merged_dataframe[S_ItemFunction.TAG_NUMBER.value]
    merged_dataframe[S_DeviceCatalogue.CATALOGUE_RNT.value] = \
    merged_dataframe.groupby(S_Item_Function_Model.MODELNO.value)[S_ItemFunction.ITEM_OBJECT_IDENTIFIER.value].rank(
        method='first')

    # Select Distinct
    distinct_dataframe = merged_dataframe.drop_duplicates()

    # Reorder columns
    final_dataframe = distinct_dataframe[[S_DeviceCatalogue.ALLOW_USE.value,
                                          S_DeviceCatalogue.TYPE.value,
                                          S_DeviceCatalogue.DESCRIPTION.value,
                                          S_DeviceCatalogue.MANUFACTURER.value,
                                          S_DeviceCatalogue.MODEL_NO.value,
                                          S_DeviceCatalogue.CLASS.value,
                                          # TODO added the 2 following rows manually
                                          S_DeviceCatalogue.LEFT.value,
                                          S_DeviceCatalogue.RIGHT.value,
                                          S_DeviceCatalogue.LEFT_MARKING.value,
                                          S_DeviceCatalogue.RIGHT_MARKING.value,
                                          S_DeviceCatalogue.SYMBOL_NAME.value,
                                          S_DeviceCatalogue.LOOP_NUMBER.value,
                                          S_DeviceCatalogue.TAG_NUMBER.value,
                                          S_DeviceCatalogue.DOCUMENT_NUMBER.value,
                                          S_DeviceCatalogue.PRODUCT_KEY.value,
                                          S_DeviceCatalogue.DATABASE_NAME.value,
                                          S_DeviceCatalogue.ITEM_OBJECT_IDENTIFIER.value,
                                          S_DeviceCatalogue.ITEM_DYNAMIC_CLASS.value,
                                          S_DeviceCatalogue.OBJECT_IDENTIFIER.value,
                                          S_DeviceCatalogue.DYNAMIC_CLASS.value,
                                          S_DeviceCatalogue.CATALOGUE_RNT.value]]

    final_dataframe[S_DeviceCatalogue.TYPE.value] = \
        DEFAULT_CELL_VALUE

    final_dataframe[S_DeviceCatalogue.CATALOGUE_RNT.value] = \
        DEFAULT_CELL_VALUE

    return final_dataframe


def create_description(row):
    description_elements = [
        row[S_ItemFunction.DESCRIPTION.value],
        row[S_ItemFunction.PRODUCT_KEY.value],
        row[S_ItemFunction.DOCUMENT_NUMBER.value],
        row[S_ItemFunction.DATABASE_NAME.value].replace('_2016R3', '')
    ]

    description_elements = [element if element is not None else '' for element in description_elements]
    joined_description = '-'.join(description_elements)

    return joined_description if joined_description.strip() != '' else 'RHLDD'
