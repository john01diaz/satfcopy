from sat_workflow_source.b_code.etl_processes.gold_layer.base_scripts.c09_Major_Equipment import \
    create_major_equipments_dataframe
from sat_workflow_source.b_code.etl_processes_wrapper.objects.etl_processes_wrapper_universes import \
    EtlProcessesWrapperUniverses
from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.etl_execution_packages.execution_databases_configuration import \
    create_database_names_view


def run_c09_major_equipment(
        etl_processes_wrapper_universe: EtlProcessesWrapperUniverses) \
        -> None:
    major_equipments_dataframe = \
        etl_processes_wrapper_universe.get_and_index_input_dataframe(
            table_name='S_Major_Equipments',
            table_type='input').table

    database_names_dataframe = \
        create_database_names_view(
            database_string='R_2016R3')

    c09_major_equipments_dataframe = \
        create_major_equipments_dataframe(
            database_names_dataframe=database_names_dataframe,
            major_equipments_dataframe=major_equipments_dataframe)

    etl_processes_wrapper_universe.register_generated_and_index_output_table(
        table=c09_major_equipments_dataframe,
        table_name='Major_Equipments',
        table_type='new_output',
        identifier_column_names=[])
