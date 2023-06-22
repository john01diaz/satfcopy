import pandas

from sat_workflow_source.b_code.etl_processes.common.constants import DEFAULT_CELL_VALUE

# Constants
CONSTANT_TRUE = "TRUE"
DATABASE_NAME_SUFFIX = "_2016R3"
DEFAULT_DESCRIPTION = "RHLDD"
DEVICE_TYPES = ['Device', 'FTA']
MINIMUM_SUM_THRESHOLD = 0

# Column names constants for S_Itemfunction (VDV)
VDV_DATABASE_NAME = "database_name"

# NOTE: manually changed from "device_type"
VDV_DEVICE_TYPE = "type"
VDV_DESCRIPTION = "description"
VDV_PRODUCT_KEY = "product_key"
VDV_DOCUMENT_NUMBER = "document_number"
VDV_ITEM_OBJECT_IDENTIFIER = "item_object_identifier"
VDV_ITEM_DYNAMIC_CLASS = "item_dynamic_class"
VDV_OBJECT_IDENTIFIER = "object_identifier"
VDV_DYNAMIC_CLASS = "dynamic_class"

# Column names constants for S_Item_Function_Model (VM)
VM_DATABASE_NAME = "database_name"
VM_MODEL_NO = "modelno"
VM_LEFT = "left"
VM_RIGHT = "right"


def create_silver_08_s_device_catalogue_dataframe(
        s_itemfunction_dataframe: pandas.DataFrame,
        s_item_function_model_dataframe: pandas.DataFrame):
    # Filtering
    filtered_s_itemfunction = \
        s_itemfunction_dataframe[
            s_itemfunction_dataframe['type'].isin(DEVICE_TYPES) &
            s_itemfunction_dataframe['location_designation'].notna()]
    
    # NOTE: Manually added
    columns_to_keep = \
        [
            'manufacturer',
            'device_type',
            'class',
            'loop_number',
            'tag_number',
            'document_number',
            'product_key',
            'database_name',
            'item_object_identifier',
            'item_dynamic_class',
            'object_identifier',
            'dynamic_class',
            'location_designation'
        ]

    # NOTE: Manually added
    filtered_s_itemfunction = \
        filtered_s_itemfunction.filter(
            items=columns_to_keep)

    filtered_s_item_function_model = \
        s_item_function_model_dataframe[
            (
                s_item_function_model_dataframe[VM_LEFT].fillna(0) +
                s_item_function_model_dataframe[VM_RIGHT].fillna(0)
            )
            > MINIMUM_SUM_THRESHOLD]
    
    # Merging
    silver_08_s_device_catalogue_dataframe = \
        pandas.merge(
            filtered_s_itemfunction,
            filtered_s_item_function_model,
            how='inner',
            on=[VDV_DATABASE_NAME, VDV_ITEM_DYNAMIC_CLASS,
                VDV_ITEM_OBJECT_IDENTIFIER, VDV_DYNAMIC_CLASS,
                VDV_OBJECT_IDENTIFIER])
    
    # Renaming
    silver_08_s_device_catalogue_dataframe = \
        silver_08_s_device_catalogue_dataframe.rename(
            columns={
                'device_type': "type",
                VDV_ITEM_OBJECT_IDENTIFIER: "item_object_identifier",
                VDV_ITEM_DYNAMIC_CLASS: "item_dynamic_class",
                VDV_OBJECT_IDENTIFIER: "object_identifier",
                VDV_DYNAMIC_CLASS: "dynamic_class"})
    
    # Generating new columns
    silver_08_s_device_catalogue_dataframe['allowuse'] = \
        CONSTANT_TRUE
    
    # NOTE: Manually filtered out - this is not in the SQL
    # # Replacing suffix in database_name
    # silver_08_s_device_catalogue_dataframe[VDV_DATABASE_NAME] = \
    #     silver_08_s_device_catalogue_dataframe[VDV_DATABASE_NAME].str.replace(DATABASE_NAME_SUFFIX, '')

    # NOTE: Manually added
    silver_08_s_device_catalogue_dataframe['description'] = \
        DEFAULT_CELL_VALUE

    # NOTE: Manually added
    silver_08_s_device_catalogue_dataframe['catalogue_rnt'] = \
        silver_08_s_device_catalogue_dataframe.sort_values('item_object_identifier').groupby('modelno').cumcount() + 1

    # NOTE: Manually added
    silver_08_s_device_catalogue_dataframe = \
        silver_08_s_device_catalogue_dataframe.drop_duplicates()

    # NOTE: Manually added
    columns_to_keep = \
        [
            'allowuse',
            'type',
            'description',
            'manufacturer',
            'modelno',
            'class',
            'left',
            'right',
            'left_marking',
            'right_marking',
            'symbol_name',
            'loop_number',
            'tag_number',
            'document_number',
            'product_key',
            'database_name',
            'item_object_identifier',
            'item_dynamic_class',
            'object_identifier',
            'dynamic_class',
            'catalogue_rnt'
        ]

    # NOTE: Manually added
    silver_08_s_device_catalogue_dataframe = \
        silver_08_s_device_catalogue_dataframe.filter(
            items=columns_to_keep)

    return \
        silver_08_s_device_catalogue_dataframe
