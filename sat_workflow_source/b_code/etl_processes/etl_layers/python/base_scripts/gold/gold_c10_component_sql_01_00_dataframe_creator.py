from enum import Enum

class DatabaseNames(
        Enum):
    DATABASE_NAME = 'database_name'


class S_Component(
        Enum):
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
    REMARKS = 'remarks'
    SEQUENCE = 'sequence'


class Component(
        Enum):
    PARENT_EQUIPMENT_NO = 'parent_equipment_no'
    EQUIPMENT_NO = 'equipment_no'
    EQUIPMENTTYPE = 'equipmenttype'
    CATALOGUENO = 'catalogueno'
    DINRAIL = 'dinrail'
    SEQUENCE = 'sequence'
    REMARKS = 'remarks'


def _get_database_names(
        database_names_dataframe):
    return database_names_dataframe[DatabaseNames.DATABASE_NAME.value]


def _filter_s_component(
        s_component_dataframe,
        database_names):
    classes = ['Instrumentation', 'Inst(Shared)', 'Elec(Shared)']
    database_name_filter = s_component_dataframe[S_Component.DATABASE_NAME.value].isin(
        database_names)
    
    class_filter = s_component_dataframe[S_Component.CLASS.value].isin(
        classes)
    
    filtered_dataframe = s_component_dataframe.loc[database_name_filter & class_filter].drop_duplicates()
    
    return filtered_dataframe[[S_Component.PARENT_EQUIPMENT_NO.value,
                               S_Component.EQUIPMENT_NO.value,
                               S_Component.EQUIPMENTTYPE.value,
                               S_Component.CATALOGUENO.value,
                               S_Component.DINRAIL.value,
                               S_Component.REMARKS.value,
                               S_Component.ITEM_OBJECT_IDENTIFIER.value]]


def _calculate_sequence(
        filtered_dataframe):
    sorted_dataframe = filtered_dataframe.sort_values(
        by=[S_Component.PARENT_EQUIPMENT_NO.value, S_Component.DINRAIL.value,
            S_Component.ITEM_OBJECT_IDENTIFIER.value])
    
    sorted_dataframe[S_Component.SEQUENCE.value] = sorted_dataframe.groupby(
            [S_Component.PARENT_EQUIPMENT_NO.value,
             S_Component.DINRAIL.value]).cumcount() + 1
    
    return sorted_dataframe[[S_Component.PARENT_EQUIPMENT_NO.value,
                             S_Component.EQUIPMENT_NO.value,
                             S_Component.EQUIPMENTTYPE.value,
                             S_Component.CATALOGUENO.value,
                             S_Component.DINRAIL.value,
                             S_Component.SEQUENCE.value,
                             S_Component.REMARKS.value]]


def create_dataframe_gold_c10_component_sql_01_00(
        input_tables):
    s_component_dataframe = \
        input_tables['Sigraph_Silver.S_Component']
    
    vw_database_names_dataframe = \
        input_tables['VW_Database_names']
    
    database_names = _get_database_names(
        vw_database_names_dataframe)
    filtered_dataframe = _filter_s_component(
        s_component_dataframe,
        database_names)
    filtered_dataframe = filtered_dataframe.drop_duplicates()
    
    final_dataframe = _calculate_sequence(
        filtered_dataframe)
    
    return final_dataframe

