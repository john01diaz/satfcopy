from sat_workflow_source.b_code.etl_processes.gold_layer.base_scripts.c10_Component import create_10_component_dataframe
from sat_workflow_source.b_code.etl_processes_wrapper.objects.etl_processes_wrapper_universes import \
    EtlProcessesWrapperUniverses
from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.etl_execution_packages.execution_databases_configuration import \
    create_database_names_view


def run_c10_component(
        etl_processes_wrapper_universe: EtlProcessesWrapperUniverses) \
        -> None:
    s_component_dataframe = \
        etl_processes_wrapper_universe.get_and_index_input_dataframe(
            table_name='S_Component',
            table_type='input').table

    database_names_dataframe = \
        create_database_names_view(
            database_string='R_2016R3')

    c10_component_dataframe = \
        create_10_component_dataframe(
            s_component_dataframe=s_component_dataframe,
            vw_database_names_dataframe=database_names_dataframe)

    etl_processes_wrapper_universe.register_generated_and_index_output_table(
        table=c10_component_dataframe,
        table_name='Component',
        table_type='new_output',
        identifier_column_names=[])
