from pandas import DataFrame

from sat_workflow_source.b_code.etl_schemas.bronze_stage.cs_layer_loop_loop_elements import CS_Layer_Loop_Loop_elements
from sat_workflow_source.b_code.etl_schemas.common.database_names import DatabaseNames
from sat_workflow_source.b_code.etl_schemas.gold_stage.instrument_type_catalogue import Instrument_Type_Catalogue


def _filter_for_null_location(
        dataframe: DataFrame) -> DataFrame:
    return dataframe[dataframe[CS_Layer_Loop_Loop_elements.CS_LOCATION_FULL_DESIGNATION.value].isnull()]


def _filter_for_valid_device_type(
        dataframe: DataFrame) -> DataFrame:
    # added brackets for removing error:
    # cannot perform 'rand_' with a dtyped [object] array and scalar of type [bool]
    
    return dataframe[(dataframe[CS_Layer_Loop_Loop_elements.CS_DEVICE_TYPE.value].notnull()) &
                     (dataframe[CS_Layer_Loop_Loop_elements.CS_DEVICE_TYPE.value].str.strip() != '')]


def _extract_type_from_element_id(
        dataframe: DataFrame) -> DataFrame:
    dataframe[Instrument_Type_Catalogue.TYPE.value] = dataframe[
        CS_Layer_Loop_Loop_elements.CS_LOOP_ELEMENT_ID.value].str.extract(
        '([A-Za-z]+)',
        expand=False)
    return dataframe


def _format_device_type(
        dataframe: DataFrame) -> DataFrame:
    dataframe[Instrument_Type_Catalogue.INSTRUMENT_TYPE.value] = dataframe[
        CS_Layer_Loop_Loop_elements.CS_DEVICE_TYPE.value].str.upper().str.strip()
    return dataframe


def _filter_for_loop_elements_in_database(
        dataframe: DataFrame,
        database_names: DataFrame) -> DataFrame:
    return dataframe[dataframe[CS_Layer_Loop_Loop_elements.LOOP_ELEMENT_DATABASE_NAME.value].isin(
            database_names[DatabaseNames.DATABASE_NAME.value])]


def _order_by_type(
        dataframe: DataFrame) -> DataFrame:
    return dataframe.sort_values(
        by=[Instrument_Type_Catalogue.TYPE.value])


def create_dataframe_gold_c02_instrument_type_catalogue_sql_01_00(
        input_tables: dict) -> DataFrame:
    loop_elements = input_tables[f"Sigraph.{CS_Layer_Loop_Loop_elements.__name__}"]
    database_names = input_tables['VW_Database_names']
    
    loop_elements = _filter_for_null_location(
        loop_elements)
    loop_elements = _filter_for_valid_device_type(
        loop_elements)
    loop_elements = _extract_type_from_element_id(
        loop_elements)
    loop_elements = _format_device_type(
        loop_elements)
    loop_elements = _filter_for_loop_elements_in_database(
        loop_elements,
        database_names)
    loop_elements = _order_by_type(
        loop_elements)
    
    return loop_elements[
        [Instrument_Type_Catalogue.TYPE.value, Instrument_Type_Catalogue.INSTRUMENT_TYPE.value]].drop_duplicates()
