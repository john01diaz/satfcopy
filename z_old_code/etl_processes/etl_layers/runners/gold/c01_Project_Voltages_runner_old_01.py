from sat_workflow_source.b_code.etl_processes.gold_layer.base_scripts.c01_project_voltages import \
    create_process_voltages_dataframe
from sat_workflow_source.b_code.etl_processes_wrapper.objects.etl_processes_wrapper_universes import \
    EtlProcessesWrapperUniverses


def run_c01_project_voltages(
        etl_processes_wrapper_universe: EtlProcessesWrapperUniverses) \
        -> None:
    cs_layer_loop_loop_elements_dataframe = \
        etl_processes_wrapper_universe.get_and_index_input_dataframe(
            table_name='CS_Layer_Loop_Loop_elements',
            table_type='input').table

    c01_project_voltages_dataframe = \
        create_process_voltages_dataframe(
            cs_layer_loop_loop_elements_dataframe=cs_layer_loop_loop_elements_dataframe)

    etl_processes_wrapper_universe.register_generated_and_index_output_table(
        table=c01_project_voltages_dataframe,
        table_name='Project_Voltages',
        table_type='new_output',
        identifier_column_names=[])
