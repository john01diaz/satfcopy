from nf_common_source.code.services.reporting_service.wrappers.run_and_log_function_wrapper import run_and_log_function
from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.etl_execution_packages.execution_databases_configuration import \
    create_database_names_view
from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.gold.gold_c03_cable_catalogue_sql_01_00_dataframe_creator import create_dataframe_gold_c03_cable_catalogue_sql_01_00
from sat_workflow_source.b_code.etl_processes_wrapper.objects.etl_processes_wrapper_universes import \
    EtlProcessesWrapperUniverses


@run_and_log_function
def run_gold_c03_cable_catalogue_sql_01_00(
        etl_processes_wrapper_universe: EtlProcessesWrapperUniverses,
        s_cablecatalogue_source: str = 'input',
        s_cablecataloguenumber_master_source: str = 'input') \
        -> None:
    cable_catalogue_dataframe = \
        etl_processes_wrapper_universe.get_and_index_input_dataframe(
            table_name='S_CableCatalogue',
            table_type=s_cablecatalogue_source).table

    cable_catalogue_number_master_dataframe = \
        etl_processes_wrapper_universe.get_and_index_input_dataframe(
            table_name='S_CableCatalogueNumber_Master',
            table_type=s_cablecataloguenumber_master_source).table

    database_names_dataframe = \
        create_database_names_view(
            database_string='R_2016R3')

    gold_c03_cable_catalogue_sql_01_00_dataframe = \
        create_dataframe_gold_c03_cable_catalogue_sql_01_00(
            cable_catalogue_dataframe=cable_catalogue_dataframe,
            cable_catalogue_number_master_dataframe=cable_catalogue_number_master_dataframe,
            database_names_dataframe=database_names_dataframe)

    etl_processes_wrapper_universe.register_generated_and_index_output_table(
        table=gold_c03_cable_catalogue_sql_01_00_dataframe,
        table_name='Cable_Catalogue',
        table_type='new_output',
        identifier_column_names=[])
