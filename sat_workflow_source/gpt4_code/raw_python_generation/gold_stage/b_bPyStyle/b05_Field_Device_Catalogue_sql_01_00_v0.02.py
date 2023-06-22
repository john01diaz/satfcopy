from enum import Enum
import pandas as pd


def device_catalogue_query(
        device_catalogue_dataframe,
        database_names_dataframe):
    # Constants
    CATALOGUE_RNT_VALUE = 1
    EMPTY_CATALOGUE_NAME = ''
    
    # Select database names
    database_names = database_names_dataframe[DatabaseNames.DATABASE_NAME.value].unique()
    
    # Filter the device catalogue dataframe
    filtered_dataframe = device_catalogue_dataframe[
        (device_catalogue_dataframe[S_DeviceCatalogue.CATALOGUE_RNT.value] == CATALOGUE_RNT_VALUE) &
        (device_catalogue_dataframe[S_DeviceCatalogue.DATABASE_NAME.value].isin(
            database_names)) &
        (device_catalogue_dataframe[S_DeviceCatalogue.CATALOGUE_NAME.value] != EMPTY_CATALOGUE_NAME)
        ].drop_duplicates()
    
    # Select distinct rows with specific columns
    result_dataframe = filtered_dataframe[[
        S_DeviceCatalogue.CATALOGUE_NAME.value,
        S_DeviceCatalogue.LEFT.value,
        S_DeviceCatalogue.LEFT_MARKING.value,
        S_DeviceCatalogue.RIGHT.value,
        S_DeviceCatalogue.RIGHT_MARKING.value,
        S_DeviceCatalogue.TAG_NUMBER.value,
        S_DeviceCatalogue.LOOP_NUMBER.value,
        S_DeviceCatalogue.DOCUMENT_NUMBER.value
        ]]
    
    return result_dataframe
