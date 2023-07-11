from sat_workflow_source.b_code.etl_schemas.common.database_names import DatabaseNames
from sat_workflow_source.b_code.etl_schemas.silver_stage.s_field_device_catalogue import S_Field_Device_Catalogue


def create_dataframe_gold_c05_field_device_catalogue_sql_01_00(
        input_tables: dict):
    s_field_device_catalogue_df = \
        input_tables['Sigraph_Silver.S_Field_Device_Catalogue']

    vw_database_names_dataframe = \
        input_tables['VW_Database_names']

    # Constants
    CATALOGUE_RNT_VALUE = 1
    EMPTY_CATALOGUE_NAME = ''

    # Select database names
    database_names = vw_database_names_dataframe[DatabaseNames.DATABASE_NAME.value].unique()

    # Filter the device catalogue dataframe
    filtered_dataframe = s_field_device_catalogue_df[
        (s_field_device_catalogue_df[S_Field_Device_Catalogue.CATALOGUE_RNT.value] == CATALOGUE_RNT_VALUE) &
        (s_field_device_catalogue_df[S_Field_Device_Catalogue.DATABASE_NAME.value].isin(
            database_names)) &
        (s_field_device_catalogue_df[S_Field_Device_Catalogue.CATALOGUE_NAME.value] != EMPTY_CATALOGUE_NAME)
        ].drop_duplicates()

    # Select distinct rows with specific columns
    result_dataframe = filtered_dataframe[[
        S_Field_Device_Catalogue.CATALOGUE_NAME.value,
        S_Field_Device_Catalogue.LEFT_PIN_DETAILS.value,
        S_Field_Device_Catalogue.LEFT_MARKING.value,
        S_Field_Device_Catalogue.RIGHT_PIN_DETAILS.value,
        S_Field_Device_Catalogue.RIGHT_MARKING.value,
        S_Field_Device_Catalogue.TAG_NUMBER.value,
        S_Field_Device_Catalogue.LOOP_NUMBER.value,
        S_Field_Device_Catalogue.DOCUMENT_NUMBER.value
    ]].drop_duplicates()

    return result_dataframe
