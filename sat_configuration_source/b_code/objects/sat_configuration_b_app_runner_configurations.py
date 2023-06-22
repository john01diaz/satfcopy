from nf_common_source.code.services.file_system_service.objects.files import Files
from nf_common_source.code.services.file_system_service.objects.folders import Folders
from nf_common_source.code.services.log_environment_utility_service.common_knowledge.environment_log_level_types import \
    EnvironmentLogLevelTypes


class SatConfigurationBAppRunnerConfigurations:
    def __init__(
            self,
            environment_log_level_type: EnvironmentLogLevelTypes.FILTERED,
            output_root_folder: Folders,
            output_folder_prefix: str,
            output_folder_suffix: str,
            etl_database_configuration_file: Files,
            etl_json_configuration_file_name: str,
            etl_json_configuration_filtered_file_name: str):
        self.environment_log_level_type = \
            environment_log_level_type

        self.output_root_folder = \
            output_root_folder

        self.output_folder_prefix = \
            output_folder_prefix

        self.output_folder_suffix = \
            output_folder_suffix

        self.etl_database_configuration_file = \
            etl_database_configuration_file
        
        self.etl_json_configuration_file_name = \
            etl_json_configuration_file_name
        
        self.etl_json_configuration_filtered_file_name = \
            etl_json_configuration_filtered_file_name
        
        self.output_folder_suffix = \
            output_folder_suffix
