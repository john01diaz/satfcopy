from sat_workflow_source.b_code.etl_processes.gold_layer.base_scripts.c12_Terminals import create_terminals_dataframe
from sat_workflow_source.b_code.etl_processes_wrapper.objects.etl_processes_wrapper_universes import \
    EtlProcessesWrapperUniverses
from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.etl_execution_packages.execution_databases_configuration import \
    create_database_names_view


def run_c12_terminals(
        etl_processes_wrapper_universe: EtlProcessesWrapperUniverses) \
        -> None:
    terminals_dataframe = \
        etl_processes_wrapper_universe.get_and_index_input_dataframe(
            table_name='S_Terminals',
            table_type='input').table

    database_names_dataframe = \
        create_database_names_view(
            database_string='R_2016R3')

    c12_terminals_dataframe = \
        create_terminals_dataframe(
            terminals_dataframe=terminals_dataframe,
            database_names_dataframe=database_names_dataframe)

    etl_processes_wrapper_universe.register_generated_and_index_output_table(
        table=c12_terminals_dataframe,
        table_name='Terminals',
        table_type='new_output',
        identifier_column_names=[])
