import pandas

# Constants
CONSTANT_TRUE = "TRUE"
DATABASE_NAME_SUFFIX = "_2016R3"
DEFAULT_DESCRIPTION = "RHLDD"
DEVICE_TYPES = ['Device', 'FTA']
MINIMUM_SUM_THRESHOLD = 0

# Column names constants for S_Itemfunction (VDV)
VDV_DATABASE_NAME = "database_name"
VDV_DEVICE_TYPE = "device_type"
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
        s_item_function_model_dataframe:  pandas.DataFrame):
    # Filtering
    filtered_s_itemfunction = \
        s_itemfunction_dataframe[
            s_itemfunction_dataframe[VDV_DEVICE_TYPE]
        .isin(DEVICE_TYPES) & s_itemfunction_dataframe[
            'location_designation'].notna()]
    filtered_s_item_function_model = \
        s_item_function_model_dataframe[
            (s_item_function_model_dataframe[VM_LEFT].fillna(
        0) + s_item_function_model_dataframe[VM_RIGHT].fillna(0))
            > MINIMUM_SUM_THRESHOLD]

    # Merging
    silver_08_s_device_catalogue_dataframe = pandas.merge(filtered_s_itemfunction, filtered_s_item_function_model,
                                                          how='inner',
                                                          on=[VDV_DATABASE_NAME, VDV_ITEM_DYNAMIC_CLASS,
                                                              VDV_ITEM_OBJECT_IDENTIFIER, VDV_DYNAMIC_CLASS,
                                                              VDV_OBJECT_IDENTIFIER])

    # Renaming
    silver_08_s_device_catalogue_dataframe = silver_08_s_device_catalogue_dataframe.rename(
        columns={VDV_DEVICE_TYPE: "Type",
                 VDV_ITEM_OBJECT_IDENTIFIER: "Item_Object_Identifier",
                 VDV_ITEM_DYNAMIC_CLASS: "Item_Dynamic_Class",
                 VDV_OBJECT_IDENTIFIER: "Object_Identifier",
                 VDV_DYNAMIC_CLASS: "Dynamic_Class"})

    # Generating new columns
    silver_08_s_device_catalogue_dataframe['AllowUse'] = \
        CONSTANT_TRUE

    # Replacing suffix in database_name
    silver_08_s_device_catalogue_dataframe[VDV_DATABASE_NAME] = \
        silver_08_s_device_catalogue_dataframe[VDV_DATABASE_NAME].str.replace(DATABASE_NAME_SUFFIX, '')

    return \
        silver_08_s_device_catalogue_dataframe
