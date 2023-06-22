from sat_workflow_source.b_code.etl_schemas.silver_stage.s_internal_wiring import S_Internal_Wiring


def create_dataframe_gold_c14_internal_wiring_sql_01_00(
        input_tables: dict):
    s_internal_wiring_dataframe = \
        input_tables['S_Internal_Wiring']
    
    database_names_dataframe = \
        input_tables['database_names']

    classes =\
        ['Instrumentation', 'Inst(Shared)', 'Elec(Shared)']
    database_names =\
        database_names_dataframe['database_name'].unique()
    
    filtered_dataframe = \
        s_internal_wiring_dataframe[
        (s_internal_wiring_dataframe[S_Internal_Wiring.CLASS.value].isin(
                classes)) &
        (s_internal_wiring_dataframe[S_Internal_Wiring.DATABASE_NAME.value].isin(
                database_names))
        ]
    
    columns_to_select = \
        [
            S_Internal_Wiring.FROM_PARENT_EQUIPMENT_NO.value,
            S_Internal_Wiring.FROM_COMPARTMENT.value,
            S_Internal_Wiring.FROM_EQUIPMENT.value,
            S_Internal_Wiring.FROM_WIRE_LINK.value,
            S_Internal_Wiring.FROM_MARKING.value,
            S_Internal_Wiring.FROM_LEFT_RIGHT.value,
            S_Internal_Wiring.TO_PARENT_EQUIPMENT_NO.value,
            S_Internal_Wiring.TO_COMPARTMENT.value,
            S_Internal_Wiring.TO_EQUIPMENT_NO.value,
            S_Internal_Wiring.TO_WIRE_LINK.value,
            S_Internal_Wiring.TO_MARKING.value,
            S_Internal_Wiring.TO_LEFT_RIGHT.value
            ]
    
    internal_wiring_dataframe = filtered_dataframe[columns_to_select].drop_duplicates()
    
    return internal_wiring_dataframe
