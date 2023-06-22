import json

from nf_common_source.code.services.file_system_service.objects.files import Files
from nf_common_source.code.services.file_system_service.objects.folders import Folders
from nf_common_source.code.services.reporting_service.reporters.log_with_datetime import log_message

from sat_workflow_source.b_code.etl_processes_wrapper.common_knowledge.origin_table_types import OriginTableTypes
from sat_workflow_source.b_code.etl_processes_wrapper.objects.table_configurations import TableConfigurations


class EtlProcessesWrapperConfigurations:
    def __init__(
            self,
            run_new_vs_original_comparison: bool,
            output_root_folder: Folders,
            output_folder_prefix: str,
            output_folder_suffix: str,
            etl_json_configuration_file: Files = None,
            etl_root_folder_path: str = None,
            file_configuration_list: list = None,
            process_configuration_list: list = None):
        # TODO - Delete when new config is implemented
        self.file_configuration_list = \
            file_configuration_list

        # TODO - Delete when new config is implemented
        self.process_configuration_list = \
            process_configuration_list

        self.run_configuration_as_json_dictionary = \
            dict()

        self.run_new_vs_original_comparison = \
            run_new_vs_original_comparison

        self.etl_root_folder_path = \
            etl_root_folder_path
        
        self.output_root_folder = \
            output_root_folder
        
        self.output_folder_prefix = \
            output_folder_prefix
        
        self.output_folder_suffix = \
            output_folder_suffix

        # TODO - remove if when new config is implemented
        if etl_json_configuration_file:
            self.set_configuration_from_json_file(
                etl_json_configuration_file=etl_json_configuration_file)

    def set_configuration_from_json_file(
            self,
            etl_json_configuration_file: Files) \
            -> None:
        try:
            with open(etl_json_configuration_file.absolute_path_string) \
                    as json_file_text_io_wrapper:
                configuration_as_json_dictionary = \
                    json.load(
                        json_file_text_io_wrapper)

                json_file_text_io_wrapper.close()

                self.run_configuration_as_json_dictionary = \
                    configuration_as_json_dictionary['etl_run']

        except Exception as exception:
            log_message(
                message=str(exception))

    def get_list_of_table_configurations(
            self) \
            -> list:
        list_of_table_configurations = \
            list()

        json_list_of_table_configurations = \
            self.run_configuration_as_json_dictionary['table_configurations']

        json_list_of_process_table_configurations = \
            self.run_configuration_as_json_dictionary['process_table_configuration']

        for json_table_configuration \
                in json_list_of_table_configurations:
            list_of_table_configurations.append(
                TableConfigurations(
                    json_table_configuration=json_table_configuration,
                    json_list_of_process_table_configurations=json_list_of_process_table_configurations))

        return \
            list_of_table_configurations

    def get_origin_type_for_table_in_process(
            self,
            table_name: str,
            process_name: str) \
            -> OriginTableTypes:
        table_configurations = \
            self.run_configuration_as_json_dictionary['table_configurations']

        table_bie_ids = \
            [
                table_configuration_dictionary['bie_table_ids']
                for table_configuration_dictionary in table_configurations
                if table_configuration_dictionary['table_names'] == table_name
            ]

        process_table_configurations = \
            self.run_configuration_as_json_dictionary['process_table_configuration']

        table_bie_id = \
            [
                process_table_configuration['bie_table_ids']
                for process_table_configuration in process_table_configurations
                if process_table_configuration['bie_table_ids'] in table_bie_ids and
                process_table_configuration['table_role_types'] == 'input'
            ][0]

        process_configurations = \
            self.run_configuration_as_json_dictionary['processes']

        bie_process_id = \
            [
                process_configuration_dictionary['bie_process_ids']
                for process_configuration_dictionary in process_configurations
                if process_configuration_dictionary['code_process_names'] == process_name
            ][0]

        input_origin_type_string =  \
            [
                process_table_configuration_dictionary['input_origin_types']
                for process_table_configuration_dictionary in process_table_configurations
                if process_table_configuration_dictionary['bie_process_ids'] == bie_process_id
                and process_table_configuration_dictionary['bie_table_ids'] == table_bie_id
            ][0]

        input_origin_type = \
            [
                input_origin_type_item for input_origin_type_item in OriginTableTypes
                if input_origin_type_item.value == input_origin_type_string
            ][0]

        return \
            input_origin_type
