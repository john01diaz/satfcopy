from nf_common_source.code.services.reporting_service.wrappers.run_and_log_function_wrapper import run_and_log_function
from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.silver.silver_c17_s_terminations_sql_01_00_dataframe_creator_fillna_test import \
    create_silver_17_s_terminations_sql_01_00_dataframe_fillna_test
from sat_workflow_source.b_code.etl_processes.etl_layers.common.etl_process_runners import EtlProcessRunners
from sat_workflow_source.b_code.etl_processes_wrapper.objects.etl_processes_wrapper_universes import \
    EtlProcessesWrapperUniverses

class SilverC17TerminationsSql0100Runners(
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

        connection_dataframe = \
            self.etl_processes_wrapper_universe.get_and_index_input_dataframe(
                table_name='S_Connection',
                process_name=process_name).table

        itemfunction_dataframe = \
            self.etl_processes_wrapper_universe.get_and_index_input_dataframe(
                table_name='S_Itemfunction',
                process_name=process_name).table

        terminals_dataframe = \
            self.etl_processes_wrapper_universe.get_and_index_input_dataframe(
                    table_name='S_Terminals',
                    process_name=process_name).table

        # silver_17_s_terminations_sql_01_00_datafram = \
        #     create_silver_17_s_terminations_sql_01_00_dataframe(
        #             connection_dataframe=connection_dataframe,
        #             itemfunction_dataframe=itemfunction_dataframe,
        #             terminals_dataframe=terminals_dataframe)

        silver_17_s_terminations_sql_01_00_datafram = \
            create_silver_17_s_terminations_sql_01_00_dataframe_fillna_test(
                connection_dataframe=connection_dataframe,
                itemfunction_dataframe=itemfunction_dataframe,
                terminals_dataframe=terminals_dataframe)


        self.etl_processes_wrapper_universe.register_generated_and_index_output_table(
            table=silver_17_s_terminations_sql_01_00_datafram,
            table_name='S_Terminations',
            identifier_column_names=[],
            process_name=process_name)
