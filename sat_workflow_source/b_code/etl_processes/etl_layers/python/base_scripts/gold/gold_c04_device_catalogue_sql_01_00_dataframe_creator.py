from sat_workflow_source.b_code.etl_schemas.common.database_names import DatabaseNames
from sat_workflow_source.b_code.etl_schemas.gold_stage.device_catalogue import Device_Catalogue
from sat_workflow_source.b_code.etl_schemas.silver_stage.s_device_catalogue import S_DeviceCatalogue

CATALOGUE_RNT_VALUE = "1"
CLASS_VALUES = ['Instrumentation', 'Inst(Shared)', 'Elec(Shared)']

def __filter_data(dataframe, database_names):
    filtered_dataframe = dataframe[
        (dataframe[S_DeviceCatalogue.CATALOGUE_RNT.value] == CATALOGUE_RNT_VALUE) &
        (dataframe[S_DeviceCatalogue.CLASS.value].isin(CLASS_VALUES)) &
        (dataframe[S_DeviceCatalogue.DATABASE_NAME.value].isin(database_names[DatabaseNames.DATABASE_NAME.value]))
    ]
    return filtered_dataframe

def __select_columns(dataframe):
    columns_to_select = [
        S_DeviceCatalogue.ALLOW_USE.value,
        S_DeviceCatalogue.TYPE.value,
        S_DeviceCatalogue.DESCRIPTION.value,
        S_DeviceCatalogue.MANUFACTURER.value,
        S_DeviceCatalogue.MODEL_NO.value,
        S_DeviceCatalogue.CLASS.value,
        S_DeviceCatalogue.LEFT.value,
        S_DeviceCatalogue.RIGHT.value,
        S_DeviceCatalogue.LEFT_MARKING.value,
        S_DeviceCatalogue.RIGHT_MARKING.value,
        S_DeviceCatalogue.SYMBOL_NAME.value,
        S_DeviceCatalogue.PRODUCT_KEY.value,
        S_DeviceCatalogue.LOOP_NUMBER.value,
        S_DeviceCatalogue.TAG_NUMBER.value,
        S_DeviceCatalogue.DOCUMENT_NUMBER.value
    ]
    return dataframe[columns_to_select].drop_duplicates()

def create_dataframe_gold_c04_device_catalogue_sql_01_00(input_tables):
    s_device_catalogue_df = input_tables[f"Sigraph_Silver.{S_DeviceCatalogue.__name__}"]
    database_names_df = input_tables["VW_Database_names"]
    filtered_dataframe = __filter_data(s_device_catalogue_df, database_names_df)
    output_dataframe = __select_columns(filtered_dataframe)
    return output_dataframe
