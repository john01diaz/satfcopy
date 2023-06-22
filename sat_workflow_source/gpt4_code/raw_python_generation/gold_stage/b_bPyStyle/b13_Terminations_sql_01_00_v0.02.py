from enum import Enum
import pandas


class DatabaseNames(
        Enum):
    DATABASE_NAME = 'database_name'


class Terminations(
        Enum):
    PARENT_EQUIPMENT_NO = 'parent_equipment_no'
    EQUIPMENT_NO = 'equipment_no'
    CABLENUMBER = 'cablenumber'
    CORE_MARKINGS = 'core_markings'
    MARKING = 'marking'
    LEFT_RIGHT = 'left_right'


class S_Terminations(
        Enum):
    CABLENUMBER = 'cablenumber'
    CLASS = 'class'
    CORE_MARKINGS = 'core_markings'
    DATABASE_NAME = 'database_name'
    EQUIPMENT_NO = 'equipment_no'
    LEFT_RIGHT = 'left_right'
    MARKING = 'marking'
    OBJECT_IDENTIFIER = 'object_identifier'
    PARENT_EQUIPMENT_NO = 'parent_equipment_no'
    PARQUET_FILE_RELATIVE_PATH = 'parquet_file_relative_path'


def filter_and_select_data(
        terminations: pandas.DataFrame,
        database_names: pandas.DataFrame):
    # Define the classes we're interested in
    classes_of_interest = ['Instrumentation', 'Inst(Shared)', 'Elec(Shared)']
    
    # Filter terminations by the classes_of_interest
    filtered_terminations = terminations[terminations[S_Terminations.CLASS.value].isin(
            classes_of_interest)]
    
    # Filter terminations where the database_name is in the database_names dataframe
    database_names_list = database_names[DatabaseNames.DATABASE_NAME.value].unique()
    filtered_terminations = filtered_terminations[filtered_terminations[S_Terminations.DATABASE_NAME.value].isin(
            database_names_list)]
    
    # Select distinct rows
    filtered_terminations = filtered_terminations[[S_Terminations.CABLENUMBER.value,
                                                   S_Terminations.CORE_MARKINGS.value,
                                                   S_Terminations.PARENT_EQUIPMENT_NO.value,
                                                   S_Terminations.EQUIPMENT_NO.value,
                                                   S_Terminations.MARKING.value,
                                                   S_Terminations.LEFT_RIGHT.value]].drop_duplicates()
    
    return filtered_terminations
