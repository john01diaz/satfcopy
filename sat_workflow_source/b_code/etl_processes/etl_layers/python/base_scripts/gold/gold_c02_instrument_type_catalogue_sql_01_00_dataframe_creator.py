from sat_workflow_source.b_code.etl_schemas.bronze_stage.filtered\
    .cs_layer_loop_loop_elements_filtered_for_02_Instrument_Type_Catalogue_sql_01_00_sql import \
    CS_Layer_Loop_Loop_elements_FilteredFor02InstrumentTypeCatalogueSql0100Sql
from sat_workflow_source.b_code.etl_schemas.common.database_names import DatabaseNames
from sat_workflow_source.b_code.etl_schemas.gold_stage.instrument_type_catalogue import Instrument_Type_Catalogue


def create_dataframe_gold_c02_instrument_type_catalogue_sql_01_00(
        input_tables: dict):
    cs_layer_loop_loop_elements_dataframe = \
        input_tables['Sigraph.CS_Layer_Loop_Loop_elements']
    
    database_names = \
        input_tables['VW_Database_names']
    # Apply transformations
    
    # Constants
    TYPE = Instrument_Type_Catalogue.TYPE.value
    INSTRUMENT_TYPE = Instrument_Type_Catalogue.INSTRUMENT_TYPE.value
    
    filtered_dataframe = cs_layer_loop_loop_elements_dataframe.loc[
        (cs_layer_loop_loop_elements_dataframe[
             CS_Layer_Loop_Loop_elements_FilteredFor02InstrumentTypeCatalogueSql0100Sql.CS_LOCATION_FULL_DESIGNATION.value
         ].isnull()) &
        (cs_layer_loop_loop_elements_dataframe[
             CS_Layer_Loop_Loop_elements_FilteredFor02InstrumentTypeCatalogueSql0100Sql.LOOP_ELEMENT_DATABASE_NAME.value].isin(
                database_names[DatabaseNames.DATABASE_NAME.value])) &
        (cs_layer_loop_loop_elements_dataframe[
             CS_Layer_Loop_Loop_elements_FilteredFor02InstrumentTypeCatalogueSql0100Sql.CS_DEVICE_TYPE.value].notnull()) &
        (cs_layer_loop_loop_elements_dataframe[
             CS_Layer_Loop_Loop_elements_FilteredFor02InstrumentTypeCatalogueSql0100Sql.CS_DEVICE_TYPE.value].str.strip() != "")
        ]
    
    filtered_dataframe[TYPE] = filtered_dataframe[
        CS_Layer_Loop_Loop_elements_FilteredFor02InstrumentTypeCatalogueSql0100Sql.CS_LOOP_ELEMENT_ID.value].str.extract(
        '([A-Za-z]+)',
        expand=False)
    filtered_dataframe[INSTRUMENT_TYPE] = filtered_dataframe[
        CS_Layer_Loop_Loop_elements_FilteredFor02InstrumentTypeCatalogueSql0100Sql.CS_DEVICE_TYPE.value].str.strip().str.upper()
    
    result_dataframe = filtered_dataframe[[TYPE, INSTRUMENT_TYPE]].drop_duplicates().sort_values(
        by=TYPE)
    
    return result_dataframe
