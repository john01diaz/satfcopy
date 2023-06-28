from sat_workflow_source.b_code.etl_processes.common.constants import DEFAULT_CELL_VALUE
from sat_workflow_source.b_code.etl_schemas.common.database_names import DatabaseNames
from sat_workflow_source.b_code.etl_schemas.gold_stage.component import Component
from sat_workflow_source.b_code.etl_schemas.silver_stage.s_component import S_Component


def create_dataframe_gold_c10_component_sql_01_00(
        input_tables: dict):
    s_component_dataframe = \
        input_tables['Sigraph_Silver.S_Component']
    
    vw_database_names_dataframe = \
        input_tables['VW_Database_names']

    class_values = ['Instrumentation', 'Inst(Shared)', 'Elec(Shared)']
    s_component_dataframe = s_component_dataframe[s_component_dataframe[S_Component.CLASS.value].isin(
            class_values)]
    s_component_dataframe = s_component_dataframe[s_component_dataframe[S_Component.DATABASE_NAME.value].isin(
            vw_database_names_dataframe[DatabaseNames.DATABASE_NAME.value])]

    s_component_dataframe.sort_values(
        by=[S_Component.PARENT_EQUIPMENT_NO.value, S_Component.DINRAIL.value, S_Component.ITEM_OBJECT_IDENTIFIER.value],
        inplace=True)
    s_component_dataframe[Component.SEQUENCE.value] = s_component_dataframe.sort_values(
        by=S_Component.ITEM_OBJECT_IDENTIFIER.value).groupby(
            [S_Component.PARENT_EQUIPMENT_NO.value, S_Component.DINRAIL.value]).cumcount() + 1
    
    component_dataframe = s_component_dataframe[[S_Component.PARENT_EQUIPMENT_NO.value,
                                                 S_Component.EQUIPMENT_NO.value,
                                                 S_Component.EQUIPMENTTYPE.value,
                                                 S_Component.CATALOGUENO.value,
                                                 S_Component.DINRAIL.value,
                                                 Component.SEQUENCE.value,
                                                 S_Component.REMARKS.value]].copy()
    component_dataframe.sort_values(
            by=[Component.SEQUENCE.value],
            inplace=True)
    component_dataframe = component_dataframe.drop_duplicates(subset=[Component.PARENT_EQUIPMENT_NO.value,
                                   Component.EQUIPMENT_NO.value,
                                   Component.EQUIPMENTTYPE.value,
                                   Component.CATALOGUENO.value,
                                   Component.DINRAIL.value,
                                   Component.REMARKS.value])
    
    component_dataframe.columns = [Component.PARENT_EQUIPMENT_NO.value,
                                   Component.EQUIPMENT_NO.value,
                                   Component.EQUIPMENTTYPE.value,
                                   Component.CATALOGUENO.value,
                                   Component.DINRAIL.value,
                                   Component.SEQUENCE.value,
                                   Component.REMARKS.value]

    

    component_dataframe.replace(
            {
                None: 'null'
                },
            inplace=True)

    # component_dataframe[Component.SEQUENCE.value] = DEFAULT_CELL_VALUE
    # component_dataframe[Component.EQUIPMENT_NO.value] = DEFAULT_CELL_VALUE
    
    return component_dataframe
