from enum import Enum
import pandas

# Define Enums
class Component(Enum):
    PARENT_EQUIPMENT_NO = 'parent_equipment_no'
    EQUIPMENT_NO = 'equipment_no'
    EQUIPMENTTYPE = 'equipmenttype'
    CATALOGUENO = 'catalogueno'
    DINRAIL = 'dinrail'
    SEQUENCE = 'sequence'
    REMARKS = 'remarks'


class S_Component(Enum):
    CATALOGUENO = 'catalogueno'
    CLASS = 'class'
    DATABASE_NAME = 'database_name'
    DINRAIL = 'dinrail'
    DYNAMIC_CLASS = 'dynamic_class'
    EQUIPMENT_NO = 'equipment_no'
    EQUIPMENTTYPE = 'equipmenttype'
    ITEM_DYNAMIC_CLASS = 'item_dynamic_class'
    ITEM_OBJECT_IDENTIFIER = 'item_object_identifier'
    OBJECT_IDENTIFIER = 'object_identifier'
    PARENT_EQUIPMENT_NO = 'parent_equipment_no'
    PARQUET_FILE_RELATIVE_PATH = 'parquet_file_relative_path'
    REMARKS = 'remarks'
    SEQUENCE = 'sequence'


def filter_dataframe_by_class(dataframe):
    class_filter_values = ['Instrumentation', 'Inst(Shared)', 'Elec(Shared)']
    return dataframe[dataframe[S_Component.CLASS.value].isin(class_filter_values)]


def filter_dataframe_by_database_name(dataframe, valid_database_names):
    return dataframe[dataframe[S_Component.DATABASE_NAME.value].isin(valid_database_names)]


def select_distinct_columns(dataframe):
    selected_columns = [col.value for col in S_Component if col.value != S_Component.SEQUENCE.value]
    return dataframe[selected_columns].drop_duplicates()


def add_sequence_column(dataframe):
    dataframe[Component.SEQUENCE.value] = dataframe.groupby([Component.PARENT_EQUIPMENT_NO.value,
                                                            Component.DINRAIL.value]).cumcount() + 1
    return dataframe


def convert_sql_to_pandas(dataframe, database_names_dataframe):
    filtered_dataframe = filter_dataframe_by_class(dataframe)
    valid_database_names = database_names_dataframe['Database_name']
    filtered_dataframe = filter_dataframe_by_database_name(filtered_dataframe, valid_database_names)
    distinct_dataframe = select_distinct_columns(filtered_dataframe)
    return add_sequence_column(distinct_dataframe)


# Use the function
Component_dataframe = pandas.DataFrame()  # Load your dataframe here
VW_Database_names_dataframe = pandas.DataFrame()  # Load your VW_Database_names dataframe here
output_dataframe = convert_sql_to_pandas(Component_dataframe, VW_Database_names_dataframe)
