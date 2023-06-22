from nf_common_source.code.services.reporting_service.wrappers.run_and_log_function_wrapper import run_and_log_function
from sat_workflow_source.b_code.etl_processes.etl_layers.common.etl_process_runners import EtlProcessRunners
from sat_workflow_source.b_code.etl_processes_wrapper.objects.etl_processes_wrapper_universes import \
    EtlProcessesWrapperUniverses
from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.silver.silver_c17_s_terminations_sql_01_00_dataframe_creator import create_silver_17_s_terminations_sql_01_00_dataframe


class SilverC17STerminationsSql0100Runners(
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

        s_connection_dataframe = \
            self.etl_processes_wrapper_universe.get_and_index_input_dataframe(
                table_name='S_Connection',
                process_name=process_name).table

        s_terminals_dataframe = \
            self.etl_processes_wrapper_universe.get_and_index_input_dataframe(
                table_name='S_Terminals',
                process_name=process_name).table

        s_item_function_dataframe = \
            self.etl_processes_wrapper_universe.get_and_index_input_dataframe(
                table_name='S_ItemFunction',
                process_name=process_name).table

        silver_17_s_terminations_sql_01_00_dataframe = \
            create_silver_17_s_terminations_sql_01_00_dataframe(
                connection_dataframe=s_connection_dataframe,
                terminals_dataframe=s_terminals_dataframe,
                itemfunction_dataframe=s_item_function_dataframe)

        self.etl_processes_wrapper_universe.register_generated_and_index_output_table(
            table=silver_17_s_terminations_sql_01_00_dataframe,
            table_name='S_Terminations',
            identifier_column_names=[],
            process_name=process_name)
