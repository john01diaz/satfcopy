import pandas as pd
from enum import Enum

# Enums
class DatabaseNames(Enum):
    DATABASE_NAME = 'database_name'

class CS_Layer_Loop_Loop_elements_FilteredForInstrumentTypeCatalogue(Enum):
    CS_LOOP_ELEMENT_ID = 'cs_loop_element_id'
    CS_DEVICE_TYPE = 'cs_device_type'
    CS_LOCATION_FULL_DESIGNATION = 'cs_location_full_designation'
    LOOP_ELEMENT_DATABASE_NAME = 'loop_element_database_name'

class Instrument_Type_Catalogue(Enum):
    TYPE = 'type'
    INSTRUMENT_TYPE = 'instrumenttype'

# Constants
TYPE = Instrument_Type_Catalogue.TYPE.value
INSTRUMENT_TYPE = Instrument_Type_Catalogue.INSTRUMENT_TYPE.value

def process_data(cs_layer_loop_loop_elements_dataframe, vw_database_names_dataframe):
    # Apply transformations
    filtered_dataframe = cs_layer_loop_loop_elements_dataframe.loc[
        (cs_layer_loop_loop_elements_dataframe[CS_Layer_Loop_Loop_elements_FilteredForInstrumentTypeCatalogue.CS_LOCATION_FULL_DESIGNATION.value].isnull()) &
        (cs_layer_loop_loop_elements_dataframe[CS_Layer_Loop_Loop_elements_FilteredForInstrumentTypeCatalogue.LOOP_ELEMENT_DATABASE_NAME.value].isin(vw_database_names_dataframe[DatabaseNames.DATABASE_NAME.value])) &
        (cs_layer_loop_loop_elements_dataframe[CS_Layer_Loop_Loop_elements_FilteredForInstrumentTypeCatalogue.CS_DEVICE_TYPE.value].notnull()) &
        (cs_layer_loop_loop_elements_dataframe[CS_Layer_Loop_Loop_elements_FilteredForInstrumentTypeCatalogue.CS_DEVICE_TYPE.value].str.strip() != "")
    ]

    filtered_dataframe[TYPE] = filtered_dataframe[CS_Layer_Loop_Loop_elements_FilteredForInstrumentTypeCatalogue.CS_LOOP_ELEMENT_ID.value].str.extract('([A-Za-z]+)', expand=False)
    filtered_dataframe[INSTRUMENT_TYPE] = filtered_dataframe[CS_Layer_Loop_Loop_elements_FilteredForInstrumentTypeCatalogue.CS_DEVICE_TYPE.value].str.strip().str.upper()

    result_dataframe = filtered_dataframe[[TYPE, INSTRUMENT_TYPE]].drop_duplicates().sort_values(by=TYPE)

    return result_dataframe


def main():
    # Read input dataframes
    cs_layer_loop_loop_elements_dataframe = pd.read_csv('cs_layer_loop_loop_elements.csv')
    vw_database_names_dataframe = pd.read_csv('vw_database_names.csv')

    # Process data
    result_dataframe = process_data(cs_layer_loop_loop_elements_dataframe, vw_database_names_dataframe)

    # Output result
    print(result_dataframe)


if __name__ == '__main__':
    main()
