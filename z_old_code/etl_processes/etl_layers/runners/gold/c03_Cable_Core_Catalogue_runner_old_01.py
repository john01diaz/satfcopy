from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.etl_execution_packages.execution_databases_configuration import \
    create_database_names_view
from sat_workflow_source.b_code.etl_processes_wrapper.objects.etl_processes_wrapper_universes import \
    EtlProcessesWrapperUniverses


def run_c03_cable_core_catalogue(
        etl_processes_wrapper_universe: EtlProcessesWrapperUniverses) \
        -> None:
    cable_catalogue_dataframe = \
        etl_processes_wrapper_universe.get_and_index_input_dataframe(
            table_name='S_CableCatalogue',
            table_type='input').table

    cable_master_dataframe = \
        etl_processes_wrapper_universe.get_and_index_input_dataframe(
            table_name='S_CableCatalogueNumber_Master',
            table_type='input').table

    cable_core_catalogue_dataframe = \
        etl_processes_wrapper_universe.get_and_index_input_dataframe(
            table_name='S_CableCoreCatalogue',
            table_type='input').table

    database_names_dataframe = \
        create_database_names_view(
            database_string='R_2016R3')

    c03_cable_core_catalogue_dataframe = \
        create_cable_core_catalogue(
            s_cable_core_catalogue_dataframe=cable_core_catalogue_dataframe,
            s_cable_catalogue_number_master_dataframe=cable_master_dataframe,
            s_cable_catalogue_dataframe=cable_catalogue_dataframe,
            vw_database_names_dataframe=database_names_dataframe)

    etl_processes_wrapper_universe.register_generated_and_index_output_table(
        table=c03_cable_core_catalogue_dataframe,
        table_name='Cable_Core_Catalogue',
        table_type='new_output',
        identifier_column_names=[])
