from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.silver.silver_c06_s_cablecatalogue_number_generation_dataframe_creator import \
    create_silver_06_s_cablecatalogue_number_generation_dataframe
from sat_workflow_source.b_code.etl_processes_wrapper.objects.etl_processes_wrapper_universes import \
    EtlProcessesWrapperUniverses


def run_silver_06_s_cablecatalogue_number_generation_sql_01_00(
        etl_processes_wrapper_universe: EtlProcessesWrapperUniverses) \
            -> None:
    s_cablecorecatalogue_dataframe = \
        etl_processes_wrapper_universe.get_and_index_input_dataframe(
            table_name='S_CableCoreCatalogue',
            table_type='input').table

    s_cablecatalogue_dataframe = \
        etl_processes_wrapper_universe.get_and_index_input_dataframe(
            table_name='S_CableCatalogue',
            table_type='input').table

    silver_06_s_cablecatalogue_number_generation_dataframe = \
        create_silver_06_s_cablecatalogue_number_generation_dataframe(
            s_cablecorecatalogue_dataframe=s_cablecorecatalogue_dataframe,
            s_cablecatalogue_dataframe=s_cablecatalogue_dataframe)

    etl_processes_wrapper_universe.register_generated_and_index_output_table(
        table=silver_06_s_cablecatalogue_number_generation_dataframe,
        table_name='S_CableCatalogueNumber_Master',
        table_type='new_output',
        #TODO Add Identifier Column Names
        identifier_column_names=[])
