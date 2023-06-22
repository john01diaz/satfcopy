import pandas
from enum import Enum


# Define the Enum for S_IO_Allocations
class S_IO_Allocations(
        Enum):
    CATALOGUENO = 'catalogueno'
    CHANNELNUMBER = 'channelnumber'
    CLASS = 'class'
    DATABASE_NAME = 'database_name'
    EQUIPMENT_NO = 'equipment_no'
    IOTYPE = 'iotype'
    OBJECT_IDENTIFIER = 'object_identifier'
    PARENT_EQUIPMENT_NO = 'parent_equipment_no'
    PARQUET_FILE_RELATIVE_PATH = 'parquet_file_relative_path'
    TAG_NUMBER = 'tag_number'


# Define the Enum for IO_Allocations
class IO_Allocations(
        Enum):
    PARENT_EQUIPMENT_NO = 'parent_equipment_no'
    EQUIPMENT_NO = 'equipment_no'
    CATALOGUENO = 'catalogueno'
    TAG_NUMBER = 'tag_number'
    IOTYPE = 'iotype'
    CHANNELNUMBER = 'channelnumber'


def filter_dataframe(
        s_io_allocations_dataframe: pandas.DataFrame,
        vw_database_names_dataframe: pandas.DataFrame) -> pandas.DataFrame:
    valid_classes = ['Instrumentation', 'Inst(Shared)', 'Elec(Shared)']
    valid_databases = vw_database_names_dataframe['Database_name'].unique()
    
    is_valid_class = s_io_allocations_dataframe[S_IO_Allocations.CLASS.value].isin(
        valid_classes)
    is_valid_database = s_io_allocations_dataframe[S_IO_Allocations.DATABASE_NAME.value].isin(
        valid_databases)
    
    return s_io_allocations_dataframe[is_valid_class & is_valid_database]


def select_distinct_columns(
        filtered_dataframe: pandas.DataFrame) -> pandas.DataFrame:
    columns_to_select = [
        S_IO_Allocations.TAG_NUMBER.value,
        S_IO_Allocations.PARENT_EQUIPMENT_NO.value,
        S_IO_Allocations.IOTYPE.value,
        S_IO_Allocations.EQUIPMENT_NO.value,
        S_IO_Allocations.CATALOGUENO.value,
        S_IO_Allocations.CHANNELNUMBER.value,
        ]
    
    return filtered_dataframe[columns_to_select].drop_duplicates()


def process_dataframe(
        s_io_allocations_dataframe: pandas.DataFrame,
        vw_database_names_dataframe: pandas.DataFrame) -> pandas.DataFrame:
    filtered_dataframe = filter_dataframe(
        s_io_allocations_dataframe,
        vw_database_names_dataframe)
    output_dataframe = select_distinct_columns(
        filtered_dataframe)
    
    return output_dataframe


# Assuming we have the input dataframes s_io_allocations_dataframe and vw_database_names_dataframe
output_dataframe = process_dataframe(
    s_io_allocations_dataframe,
    vw_database_names_dataframe)
