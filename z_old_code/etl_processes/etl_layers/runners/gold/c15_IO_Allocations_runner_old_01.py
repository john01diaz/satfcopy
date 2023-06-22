from sat_workflow_source.b_code.etl_processes.gold_layer.base_scripts.c15_IO_Allocations import \
    create_io_allocations_dataframe
from sat_workflow_source.b_code.etl_processes_wrapper.objects.etl_processes_wrapper_universes import \
    EtlProcessesWrapperUniverses
from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.etl_execution_packages.execution_databases_configuration import \
    create_database_names_view


def run_c15_io_allocations(
        etl_processes_wrapper_universe: EtlProcessesWrapperUniverses) \
        -> None:
    s_io_allocations_dataframe = \
        etl_processes_wrapper_universe.get_and_index_input_dataframe(
            table_name='S_IO_Allocations',
            table_type='input').table

    database_names_dataframe = \
        create_database_names_view(
            database_string='R_2016R3')

    c15_io_allocations_dataframe = \
        create_io_allocations_dataframe(
            s_io_allocations_dataframe=s_io_allocations_dataframe,
            database_names_dataframe=database_names_dataframe)

    etl_processes_wrapper_universe.register_generated_and_index_output_table(
        table=c15_io_allocations_dataframe,
        table_name='IO_Allocations',
        table_type='new_output',
        identifier_column_names=[])
