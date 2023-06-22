from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.gold.gold_c15_io_allocations_sql_01_00_dataframe_creator import \
    create_dataframe_gold_c15_io_allocations_sql_01_00
from sat_workflow_source.b_code.etl_processes_wrapper.objects.etl_processes_wrapper_universes import \
    EtlProcessesWrapperUniverses
from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.etl_execution_packages.execution_databases_configuration import \
    create_database_names_view


def run_gold_c15_io_allocations_sql_01_00(
        etl_processes_wrapper_universe: EtlProcessesWrapperUniverses) \
        -> None:
    s_io_allocations_dataframe = \
        etl_processes_wrapper_universe.get_and_index_input_dataframe(
            table_name='S_IO_Allocations',
            table_type='input').table

    database_names_dataframe = \
        create_database_names_view(
            database_string='R_2016R3')

    gold_c15_io_allocations_sql_01_00_dataframe = \
        create_dataframe_gold_c15_io_allocations_sql_01_00(
            s_io_allocations_dataframe=s_io_allocations_dataframe,
            database_names_dataframe=database_names_dataframe)

    etl_processes_wrapper_universe.register_generated_and_index_output_table(
        table=gold_c15_io_allocations_sql_01_00_dataframe,
        table_name='IO_Allocations',
        table_type='new_output',
        identifier_column_names=[])
