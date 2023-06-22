from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.etl_execution_packages.execution_databases_configuration import \
    create_database_names_view
from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.gold.gold_c05_field_device_catalogue_sql_01_00_dataframe_creator import \
    create_dataframe_gold_c05_field_device_catalogue_sql_01_00

from sat_workflow_source.b_code.etl_processes_wrapper.objects.etl_processes_wrapper_universes import \
    EtlProcessesWrapperUniverses


def run_gold_c05_field_device_catalogue_sql_01_00(
        etl_processes_wrapper_universe: EtlProcessesWrapperUniverses) \
        -> None:
    s_field_device_catalogue_df = \
        etl_processes_wrapper_universe.get_and_index_input_dataframe(
            table_name='S_Field_Device_Catalogue',
            table_type='input').table

    database_names_dataframe = \
        create_database_names_view(
            database_string='R_2016R3')

    gold_c05_field_device_catalogue_sql_01_00_dataframe = \
        create_dataframe_gold_c05_field_device_catalogue_sql_01_00(
            s_field_device_catalogue_df=s_field_device_catalogue_df,
            vw_database_names_dataframe=database_names_dataframe)

    etl_processes_wrapper_universe.register_generated_and_index_output_table(
        table=gold_c05_field_device_catalogue_sql_01_00_dataframe,
        table_name='Field_Device_Catalogue',
        table_type='new_output',
        identifier_column_names=[])
