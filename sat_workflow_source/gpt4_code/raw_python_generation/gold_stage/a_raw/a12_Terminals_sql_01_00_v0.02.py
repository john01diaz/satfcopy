from enum import Enum
import pandas


class DatabaseNames(
        Enum):
    DATABASE_NAME = 'database_name'

class Terminals(
        Enum):
    PARENT_EQUIPMENT_NO = 'parent_equipment_no'
    EQUIPMENT_NO = 'equipment_no'
    SEQUENCE = 'sequence'
    MARKING = 'marking'

class S_Terminals(
        Enum):
    CLASS = 'class'
    DATABASE_NAME = 'database_name'
    DYNAMIC_CLASS = 'dynamic_class'
    EQUIPMENT_NO = 'equipment_no'
    MARKING = 'marking'
    OBJECT_IDENTIFIER = 'object_identifier'
    PARENT_EQUIPMENT_NO = 'parent_equipment_no'
    PARQUET_FILE_RELATIVE_PATH = 'parquet_file_relative_path'
    SEQUENCE = 'sequence'


def dense_rank(
        df,
        partition_columns,
        order_by_column):
    df = df.sort_values(
        by=partition_columns + [order_by_column])
    df[Terminals.SEQUENCE.value] = df.groupby(
        partition_columns).rank(
        method='min')
    return df


def filter_and_select_data(
        terminals: pandas.DataFrame,
        database_names: pandas.DataFrame):
    # Define the classes we're interested in
    classes_of_interest = ['Instrumentation', 'Inst(Shared)', 'Elec(Shared)']
    
    # Filter terminals by the classes_of_interest
    filtered_terminals = terminals[terminals[S_Terminals.CLASS.value].isin(
        classes_of_interest)]
    
    # Filter terminals where the database_name is in the database_names dataframe
    database_names_list = database_names[DatabaseNames.DATABASE_NAME.value].unique()
    filtered_terminals = filtered_terminals[filtered_terminals[S_Terminals.DATABASE_NAME.value].isin(
        database_names_list)]
    
    # Select distinct rows
    filtered_terminals = filtered_terminals[[S_Terminals.PARENT_EQUIPMENT_NO.value,
                                             S_Terminals.EQUIPMENT_NO.value,
                                             S_Terminals.MARKING.value]].drop_duplicates()
    
    # Apply the dense_rank function
    final_terminals = dense_rank(
        df=filtered_terminals,
        partition_columns=[S_Terminals.PARENT_EQUIPMENT_NO.value, S_Terminals.EQUIPMENT_NO.value],
        order_by_column=S_Terminals.MARKING.value)
    
    return final_terminals
