from sat_workflow_source.b_code.etl_schemas.common.database_names import DatabaseNames
from sat_workflow_source.b_code.etl_schemas.gold_stage.device_catalogue import Device_Catalogue
from sat_workflow_source.b_code.etl_schemas.silver_stage.s_device_catalogue import S_DeviceCatalogue

# TODO - DELETE - Used to test temporary view tables
def create_dataframe_gold_c04_device_catalogue_sql_01_00_part_1(
        input_tables: dict):
    s_device_catalogue_dataframe = \
        input_tables['Sigraph_Silver.S_DeviceCatalogue']

    database_names_dataframe = \
        input_tables['database_names']

    database_dataframe_filtered = \
        filter_database_names(
            database_names_dataframe)

    device_catalogue_dataframe_filtered = \
        filter_device_catalogue(
            s_device_catalogue_dataframe,
            database_dataframe_filtered)


    return \
        device_catalogue_dataframe_filtered


def filter_database_names(
        database_dataframe):
    return database_dataframe[DatabaseNames.DATABASE_NAME.value]


def filter_device_catalogue(
        device_catalogue_dataframe,
        database_dataframe_filtered):
    class_values = ['Instrumentation', 'Inst(Shared)', 'Elec(Shared)']
    database_names = database_dataframe_filtered.unique()
    
    filtered_device_catalogue = device_catalogue_dataframe[
        (device_catalogue_dataframe[S_DeviceCatalogue.CATALOGUE_RNT.value] == 1) &
        (device_catalogue_dataframe[S_DeviceCatalogue.CLASS.value].isin(
            class_values)) &
        (device_catalogue_dataframe[S_DeviceCatalogue.DATABASE_NAME.value].isin(
            database_names))
        ]
    
    return filtered_device_catalogue


def select_columns(
        device_catalogue_dataframe_filtered):
    column_names = [
        Device_Catalogue.ALLOWUSE.value,
        Device_Catalogue.TYPE.value,
        Device_Catalogue.DESCRIPTION.value,
        Device_Catalogue.MANUFACTURER.value,
        Device_Catalogue.MODELNO.value,
        Device_Catalogue.CLASS.value,
        Device_Catalogue.LEFT.value,
        Device_Catalogue.RIGHT.value,
        Device_Catalogue.LEFT_MARKING.value,
        Device_Catalogue.RIGHT_MARKING.value,
        Device_Catalogue.SYMBOL_NAME.value,
        Device_Catalogue.PRODUCT_KEY.value,
        Device_Catalogue.LOOP_NUMBER.value,
        Device_Catalogue.TAG_NUMBER.value,
        Device_Catalogue.DOCUMENT_NUMBER.value
        ]
    
    dataframe_selected_columns = device_catalogue_dataframe_filtered[column_names]
    dataframe_selected_columns_unique = dataframe_selected_columns.drop_duplicates()
    
    return dataframe_selected_columns_unique
