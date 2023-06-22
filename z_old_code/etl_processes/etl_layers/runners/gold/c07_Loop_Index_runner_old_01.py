from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.etl_execution_packages.execution_databases_configuration import \
    create_database_names_view
from sat_workflow_source.b_code.etl_processes.gold_layer.base_scripts.c07_Loop_Index_v0_02 import \
    create_loop_index_dataframe
from sat_workflow_source.b_code.etl_processes_wrapper.objects.etl_processes_wrapper_universes import \
    EtlProcessesWrapperUniverses


def run_c07_loop_index(
        etl_processes_wrapper_universe: EtlProcessesWrapperUniverses) \
        -> None:
    loop_index_dataframe = \
        etl_processes_wrapper_universe.get_and_index_input_dataframe(
            table_name='S_Loop_Index',
            table_type='input').table

    database_names_dataframe = \
        create_database_names_view(
            database_string='R_2016R3')

    c07_loop_index_dataframe = \
        create_loop_index_dataframe(
            loop_index_dataframe=loop_index_dataframe,
            vw_database_names_dataframe=database_names_dataframe)

    etl_processes_wrapper_universe.register_generated_and_index_output_table(
        table=c07_loop_index_dataframe,
        table_name='Loop_Index',
        table_type='new_output',
        identifier_column_names=[])
