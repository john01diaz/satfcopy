from nf_common_source.code.services.reporting_service.wrappers.run_and_log_function_wrapper import run_and_log_function

from sat_workflow_source.b_code.etl_processes.common.table_columns_filterer import filter_table_columns
from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.silver.silver_c11_s_loop_index_sql_01_00_dataframe_creator import create_silver_c11_s_loop_index_sql_01_00_dataframe

from sat_workflow_source.b_code.etl_processes_wrapper.objects.etl_processes_wrapper_universes import \
    EtlProcessesWrapperUniverses
from sat_workflow_source.b_code.etl_schemas.bronze_stage.filtered.loop_filtered_for_11_S_Loop_Index_sql_01_00_sql import \
    LoopFilteredFor11SLoopIndexSql0100Sql


@run_and_log_function
def run_silver_c11_s_loop_index_sql_01_00(
        etl_processes_wrapper_universe: EtlProcessesWrapperUniverses) \
        -> None:
    layer_dataframe = \
        etl_processes_wrapper_universe.get_and_index_input_dataframe(
            table_name='Layer',
            table_type='input').table

    loop_dataframe = \
        etl_processes_wrapper_universe.get_and_index_input_dataframe(
            table_name='Loop',
            table_type='input').table

    loop_table_for_loop_index = \
        filter_table_columns(
            input_table=loop_dataframe,
            columns_to_include=[e.value for e in LoopFilteredFor11SLoopIndexSql0100Sql])

    pbs_dataframe = \
        etl_processes_wrapper_universe.get_and_index_input_dataframe(
            table_name='PBS_Table',
            table_type='input').table

    # silver_c11_s_loop_index_sql_01_00_dataframe = \
    #     create_silver_c11_s_loop_index_sql_01_00_dataframe(
    #         layer_dataframe=layer_dataframe,
    #         loop_dataframe=loop_table_for_loop_index,
    #         pbs_dataframe=pbs_dataframe)

    silver_c11_s_loop_index_sql_01_00_dataframe = \
        create_silver_c11_s_loop_index_sql_01_00_dataframe(
            layer_dataframe=layer_dataframe,
            loop_dataframe=loop_table_for_loop_index,
            pbs_dataframe=pbs_dataframe)

    etl_processes_wrapper_universe.register_generated_and_index_output_table(
        table=silver_c11_s_loop_index_sql_01_00_dataframe,
        table_name='S_Loop_Index',
        table_type='new_output',
        identifier_column_names=['object_identifier'])
