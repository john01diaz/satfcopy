import pandas
from nf_common_source.code.services.reporting_service.reporters.log_with_datetime import log_message
from nf_common_source.code.services.reporting_service.wrappers.run_and_log_function_wrapper import run_and_log_function
from sat_workflow_source.b_code.etl_processes.etl_layers.common.etl_process_runners import EtlProcessRunners
from sat_workflow_source.b_code.etl_processes_wrapper.objects.etl_processes_wrapper_configurations import \
    EtlProcessesWrapperConfigurations
from sat_workflow_source.b_code.etl_processes_wrapper.objects.etl_processes_wrapper_registries import \
    EtlProcessesWrapperRegistries
from sat_workflow_source.b_code.etl_processes_wrapper.objects.b_tables import BTables
from sat_workflow_source.b_code.etl_processes_wrapper.objects.table_configurations import TableConfigurations


class EtlProcessesWrapperUniverses:
    def __init__(
            self,
            etl_processes_wrapper_configuration: EtlProcessesWrapperConfigurations):
        self.etl_processes_wrapper_registry = \
            EtlProcessesWrapperRegistries(
                self)

        self.etl_processes_wrapper_configuration = \
            etl_processes_wrapper_configuration

        self.load_tables()

    def __enter__(
            self):
        return \
            self

    def __exit__(
            self,
            exception_type,
            exception_value,
            traceback):
        pass

    @run_and_log_function
    # TODO This needs handle multiple roles for the same file in the future.
    def load_tables(self):
        list_of_table_configurations = \
            self.etl_processes_wrapper_configuration.get_list_of_table_configurations()

        for table_configuration \
                in list_of_table_configurations:
            self.load_and_register_source_table(
                table_configuration=table_configuration)

    @run_and_log_function
    def process_files(
            self) \
            -> None:
        process_configurations = \
            self.etl_processes_wrapper_configuration.run_configuration_as_json_dictionary['processes']

        sorted_list_of_process_configurations = \
            sorted(
                process_configurations,
                key=lambda process_configuration_dictionary: process_configuration_dictionary['run_order'])

        for process_configuration in sorted_list_of_process_configurations:
            self.__run_process_configuration(
                process_configuration=process_configuration)

    def __run_process_configuration(
            self,
            process_configuration: dict):
        code_process_name = \
            process_configuration['code_process_names']

        log_message(
            'start - ' + code_process_name)

        if process_configuration['process_names'] == 'bieize':
            return
    
        etl_process_runner = \
            EtlProcessRunners(
                etl_processes_wrapper_universe=self,
                process_configuration=process_configuration)
    
        etl_process_runner.run()

        log_message(
            'end - ' + code_process_name)

    def load_and_register_source_table(
            self,
            table_configuration: TableConfigurations) \
            -> None:
        self.etl_processes_wrapper_registry.load_and_register_source_table(
            table_configuration=table_configuration)

    def register_generated_and_index_output_table(
            self,
            table: pandas.DataFrame,
            table_name: str,
            identifier_column_names: list,
            process_name: str) \
            -> None:
        self.etl_processes_wrapper_registry.register_generated_and_index_output_table(
            etl_table=table,
            etl_table_name=table_name,
            identifier_column_names=identifier_column_names,
            process_name=process_name)

    def get_and_index_input_dataframe(
            self,
            table_name: str,
            process_name: str) \
            -> BTables:
        return \
            self.etl_processes_wrapper_registry.get_and_index_input_dataframe(
                table_name=table_name,
                process_name=process_name)

    @run_and_log_function
    def bieize(
            self) \
            -> None:
        self.etl_processes_wrapper_registry.bieize()

    @run_and_log_function
    def export_report_files(
            self) \
            -> None:
        self.etl_processes_wrapper_registry.export_report_files()


