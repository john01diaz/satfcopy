from nf_common_source.code.services.reporting_service.wrappers.run_and_log_function_wrapper import run_and_log_function

from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.etl_execution_packages.execution_databases_configuration import \
    create_database_names_view
from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.gold.gold_c06_io_catalogue_sql_01_00_dataframe_creator import create_dataframe_gold_c06_io_catalogue_sql_01_00
from sat_workflow_source.b_code.etl_processes.etl_layers.common.etl_process_runners import EtlProcessRunners
from sat_workflow_source.b_code.etl_processes_wrapper.objects.etl_processes_wrapper_universes import \
    EtlProcessesWrapperUniverses


class Gold06IOCatalogueSql0100Runners(
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

        io_catalogue_dataframe = \
            self.etl_processes_wrapper_universe.get_and_index_input_dataframe(
                table_name='S_IO_Catalogue',
                process_name=process_name).table

        database_names_dataframe = \
            create_database_names_view(
                    database_string='R_2016R3')

        silver_08_s_device_catalogue_dataframe_dataframe = \
            create_dataframe_gold_c06_io_catalogue_sql_01_00(
                    io_catalogue_dataframe=io_catalogue_dataframe,
                    vw_database_names_dataframe=database_names_dataframe)

        self.etl_processes_wrapper_universe.register_generated_and_index_output_table(
            table=silver_08_s_device_catalogue_dataframe_dataframe,
            table_name='IO_Catalogue',
            identifier_column_names=[],
            process_name=process_name)

