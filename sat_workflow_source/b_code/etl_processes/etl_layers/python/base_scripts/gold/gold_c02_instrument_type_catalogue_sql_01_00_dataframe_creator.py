from sat_workflow_source.b_code.etl_schemas.bronze_stage.cs_layer_loop_loop_elements import CS_Layer_Loop_Loop_elements
from sat_workflow_source.b_code.etl_schemas.common.database_names import DatabaseNames
from sat_workflow_source.b_code.etl_schemas.gold_stage.instrument_type_catalogue import Instrument_Type_Catalogue
from pandas import DataFrame
import pandas
from enum import Enum


def _extract_type(dataframe, input_column, output_column):
    dataframe[output_column] = dataframe[input_column].str.extract('([A-Za-z]+)')
    return dataframe


def _convert_to_upper_and_trim(dataframe, column_name):
    dataframe[column_name] = dataframe[column_name].str.upper().str.strip()
    return dataframe


def _filter_rows(dataframe, column_name, database_names):
    dataframe = dataframe[dataframe[column_name] == 'R_2016R3']
    return dataframe


def _remove_all_values_not_null(dataframe: DataFrame, column_name):
    dataframe = dataframe[dataframe[column_name].isna()]
    return dataframe


def _remove_nulls_and_empty(dataframe: DataFrame, column_name):
    dataframe = dataframe[dataframe[column_name].notna()]
    dataframe = dataframe[dataframe[column_name].str.strip() != ""]
    return dataframe


def _order_by(dataframe, column_name):
    dataframe = dataframe.sort_values(column_name)
    return dataframe


def _rename_columns(dataframe, map: dict):
    dataframe = dataframe.rename(map, axis=1)
    return dataframe


def create_dataframe_gold_c02_instrument_type_catalogue_sql_01_00(input_tables):
    loop_elements = input_tables[f"Sigraph.{CS_Layer_Loop_Loop_elements.__name__}"]
    database_names = input_tables['VW_Database_names']

    column_rename_dict = {
        CS_Layer_Loop_Loop_elements.CS_DEVICE_TYPE.value: Instrument_Type_Catalogue.INSTRUMENT_TYPE.value
    }

    dataframe = _filter_rows(loop_elements, CS_Layer_Loop_Loop_elements.LOOP_ELEMENT_DATABASE_NAME.value,
                             database_names)

    dataframe = _extract_type(dataframe, CS_Layer_Loop_Loop_elements.CS_LOOP_ELEMENT_ID.value,
                              Instrument_Type_Catalogue.TYPE.value)
    dataframe = _convert_to_upper_and_trim(dataframe, CS_Layer_Loop_Loop_elements.CS_DEVICE_TYPE.value)

    dataframe = _order_by(dataframe, Instrument_Type_Catalogue.TYPE.value)
    dataframe = _rename_columns(dataframe, column_rename_dict)

    dataframe = _remove_all_values_not_null(dataframe, CS_Layer_Loop_Loop_elements.CS_LOCATION_FULL_DESIGNATION.value)
    dataframe = _remove_nulls_and_empty(dataframe, Instrument_Type_Catalogue.TYPE.value)
    dataframe = _remove_nulls_and_empty(dataframe, Instrument_Type_Catalogue.INSTRUMENT_TYPE.value)

    dataframe = dataframe[[Instrument_Type_Catalogue.TYPE.value, Instrument_Type_Catalogue.INSTRUMENT_TYPE.value]]
    dataframe = dataframe.drop_duplicates()

    return dataframe
