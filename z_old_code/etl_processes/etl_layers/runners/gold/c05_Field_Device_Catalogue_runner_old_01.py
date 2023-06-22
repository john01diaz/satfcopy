from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.etl_execution_packages.execution_databases_configuration import \
    create_database_names_view
from sat_workflow_source.b_code.etl_processes.gold_layer.base_scripts.c05_Field_Device_Catalogue import \
    create_field_device_catalogue_dataframe
from sat_workflow_source.b_code.etl_processes_wrapper.objects.etl_processes_wrapper_universes import \
    EtlProcessesWrapperUniverses


def run_c05_field_device_catalogue(
        etl_processes_wrapper_universe: EtlProcessesWrapperUniverses) \
        -> None:
    s_field_device_catalogue_df = \
        etl_processes_wrapper_universe.get_and_index_input_dataframe(
            table_name='S_Field_Device_Catalogue',
            table_type='input').table

    database_names_dataframe = \
        create_database_names_view(
            database_string='R_2016R3')

    c05_field_device_catalogue_dataframe = \
        create_field_device_catalogue_dataframe(
            s_field_device_catalogue_df=s_field_device_catalogue_df,
            vw_database_names_df=database_names_dataframe)

    etl_processes_wrapper_universe.register_generated_and_index_output_table(
        table=c05_field_device_catalogue_dataframe,
        table_name='Field_Device_Catalogue',
        table_type='new_output',
        identifier_column_names=[])
