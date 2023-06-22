from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.silver.silver_c18_s_internal_wiring_sql_01_00_dataframe_creator import \
    create_silver_18_s_internal_wiring_sql_01_00_dataframe
from sat_workflow_source.b_code.etl_processes_wrapper.objects.etl_processes_wrapper_universes import \
    EtlProcessesWrapperUniverses


def run_silver_18_s_internal_wiring_sql_01_00(
        etl_processes_wrapper_universe: EtlProcessesWrapperUniverses) \
        -> None:
    
    s_connection_dataframe = \
        etl_processes_wrapper_universe.get_and_index_input_dataframe(
            table_name='S_Connection',
            table_type='input').table
    
    s_terminals_dataframe = \
        etl_processes_wrapper_universe.get_and_index_input_dataframe(
            table_name='S_Terminals',
            table_type='input').table
    
    silver_18_s_internal_wiring_sql_01_00_dataframe = \
        create_silver_18_s_internal_wiring_sql_01_00_dataframe(
            s_connection_dataframe=s_connection_dataframe,
            s_terminals_dataframe=s_terminals_dataframe)
    
    etl_processes_wrapper_universe.register_generated_and_index_output_table(
        table=silver_18_s_internal_wiring_sql_01_00_dataframe,
        table_name='S_Internal_Wiring',
        table_type='new_output',
        # TODO Add Identifier Column Names
        identifier_column_names=[])
