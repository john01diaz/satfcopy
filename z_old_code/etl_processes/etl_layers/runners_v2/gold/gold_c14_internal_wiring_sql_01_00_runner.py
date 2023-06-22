from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.gold.gold_c14_internal_wiring_sql_01_00_dataframe_creator import create_dataframe_gold_c14_internal_wiring_sql_01_00
from sat_workflow_source.b_code.etl_processes_wrapper.objects.etl_processes_wrapper_universes import \
    EtlProcessesWrapperUniverses
from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.etl_execution_packages.execution_databases_configuration import \
    create_database_names_view


def run_gold_c14_internal_wiring_sql_01_00(
        etl_processes_wrapper_universe: EtlProcessesWrapperUniverses,
        s_internal_wiring_source: str = 'input') \
        -> None:
    s_internal_wiring_dataframe = \
        etl_processes_wrapper_universe.get_and_index_input_dataframe(
            table_name='S_Internal_Wiring',
            table_type=s_internal_wiring_source).table

    database_names_dataframe = \
        create_database_names_view(
            database_string='R_2016R3')

    gold_c14_internal_wiring_sql_01_00_dataframe = \
        create_dataframe_gold_c14_internal_wiring_sql_01_00(
            s_internal_wiring_dataframe=s_internal_wiring_dataframe,
            database_names_dataframe=database_names_dataframe)

    etl_processes_wrapper_universe.register_generated_and_index_output_table(
        table=gold_c14_internal_wiring_sql_01_00_dataframe,
        table_name='Interal_Wiring',
        table_type='new_output',
        identifier_column_names=[])
