from nf_common_source.code.services.reporting_service.wrappers.run_and_log_function_wrapper import run_and_log_function

from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.etl_execution_packages.execution_databases_configuration import \
    create_database_names_view
from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.gold.gold_c07_loop_index_sql_01_00_dataframe_creator import create_dataframe_gold_c07_loop_index_sql_01_00_v0_02

from sat_workflow_source.b_code.etl_processes.etl_layers.common.etl_process_runners import EtlProcessRunners
from sat_workflow_source.b_code.etl_processes_wrapper.objects.etl_processes_wrapper_universes import \
    EtlProcessesWrapperUniverses


class GoldC07LoopIndexSql0100Runners(
        EtlProcessRunners):
    def __init__(
            self,
            etl_processes_wrapper_universe: EtlProcessesWrapperUniverses):
        super().__init__(
            etl_processes_wrapper_universe=etl_processes_wrapper_universe)

    @run_and_log_function
    def run(
            self) \
            -> None:
        process_name = \
            self.__class__.__name__

        loop_index_dataframe = \
            self.etl_processes_wrapper_universe.get_and_index_input_dataframe(
                    table_name='S_Loop_Index',
                    process_name=process_name).table

        database_names_dataframe = \
            create_database_names_view(
                database_string='R_2016R3')
        
        gold_c07_loop_index_sql_01_00_v0_02_dataframe = \
            create_dataframe_gold_c07_loop_index_sql_01_00_v0_02(
                loop_index_dataframe=loop_index_dataframe,
                database_names_dataframe=database_names_dataframe)
        
        self.etl_processes_wrapper_universe.register_generated_and_index_output_table(
            table=gold_c07_loop_index_sql_01_00_v0_02_dataframe,
            table_name='Loop_Index',
            identifier_column_names=[],
            process_name=process_name)
