from sat_workflow_source.b_code.etl_processes.gold_layer.base_scripts.c13_Terminations import \
    create_terminations_dataframe
from sat_workflow_source.b_code.etl_processes_wrapper.objects.etl_processes_wrapper_universes import \
    EtlProcessesWrapperUniverses
from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.etl_execution_packages.execution_databases_configuration import \
    create_database_names_view


def run_c13_terminations(
        etl_processes_wrapper_universe: EtlProcessesWrapperUniverses) \
        -> None:
    terminations_dataframe = \
        etl_processes_wrapper_universe.get_and_index_input_dataframe(
            table_name='S_Terminations',
            table_type='input').table

    database_names_dataframe = \
        create_database_names_view(
            database_string='R_2016R3')

    c13_terminations_dataframe = \
        create_terminations_dataframe(
            terminations_dataframe=terminations_dataframe,
            database_names_dataframe=database_names_dataframe)

    etl_processes_wrapper_universe.register_generated_and_index_output_table(
        table=c13_terminations_dataframe,
        table_name='Terminations',
        table_type='new_output',
        identifier_column_names=[])
