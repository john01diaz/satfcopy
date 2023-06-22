import inspect

from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.gold.gold_c01_project_voltages_sql_01_00_dataframe_creator import create_dataframe_gold_c01_project_voltages_sql_01_00
from sat_workflow_source.b_code.etl_processes_wrapper.objects.etl_processes_wrapper_universes import \
    EtlProcessesWrapperUniverses


def run_gold_c01_project_voltages_sql_01_00(
        etl_processes_wrapper_universe: EtlProcessesWrapperUniverses) \
        -> None:
    cs_layer_loop_loop_elements_dataframe = \
        etl_processes_wrapper_universe.get_and_index_input_dataframe(
            table_name='CS_Layer_Loop_Loop_elements',
            table_type='input').table

    gold_c01_project_voltages_sql_01_00_dataframe = \
        create_dataframe_gold_c01_project_voltages_sql_01_00(
            cs_layer_loop_loop_elements_dataframe=cs_layer_loop_loop_elements_dataframe)

    frame = inspect.currentframe()
    
    x = inspect.getframeinfo(
        frame).function

    etl_processes_wrapper_universe.register_generated_and_index_output_table(
        table=gold_c01_project_voltages_sql_01_00_dataframe,
        table_name='Project_Voltages',
        identifier_column_names=[],
        process_name=x)
