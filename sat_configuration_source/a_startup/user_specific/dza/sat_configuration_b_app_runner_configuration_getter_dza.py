import os
from nf_common_source.code.services.file_system_service.objects.files import Files
from nf_common_source.code.services.file_system_service.objects.folders import Folders
from nf_common_source.code.services.log_environment_utility_service.common_knowledge.environment_log_level_types import \
    EnvironmentLogLevelTypes
from sat_configuration_source.b_code.objects.sat_configuration_b_app_runner_configurations import \
    SatConfigurationBAppRunnerConfigurations


def get_sat_configuration_b_app_runner_configuration_dza() \
        -> SatConfigurationBAppRunnerConfigurations:
    configurations_folder = \
        __get_configurations_folder()

    etl_database_configuration_file = \
        __get_etl_database_configuration_file(
            configurations_folder=configurations_folder,
            etl_database_configuration_filename='database_configuration v20230717 v0.07 AGu - for architecture refactoring.accdb')

    sat_configuration_b_app_runner_configuration = \
        SatConfigurationBAppRunnerConfigurations(
            environment_log_level_type=EnvironmentLogLevelTypes.FILTERED,
            output_root_folder=configurations_folder,
            output_folder_prefix='json_configurations',
            output_folder_suffix='sqlite_error',
            etl_database_configuration_file=etl_database_configuration_file,
            etl_json_configuration_file_name='etl_run_configuration_full_sqlite_error.json',
            etl_json_configuration_filtered_file_name='etl_run_configuration_filtered_sqlite_error.json')
    
    return \
        sat_configuration_b_app_runner_configuration


def __get_configurations_folder() \
        -> Folders:
    user_initials = \
        'DZa'

    b_root_folder_path = \
        os.path.join(
            'c:',
            os.sep,
            'bWa')

    etl_root_folder_path = \
        os.path.join(
            b_root_folder_path,
            user_initials,
            'etl')

    configurations_folder_path = \
        os.path.join(
            etl_root_folder_path,
            'configurations')

    configurations_folder = \
        Folders(
            absolute_path_string=configurations_folder_path)

    return \
        configurations_folder


def __get_etl_database_configuration_file(
        configurations_folder: Folders,
        etl_database_configuration_filename: str) \
        -> Files:
    etl_database_configuration_file_path = \
        os.path.join(
            configurations_folder.absolute_path_string,
            '20230719 - Annes updates',
            etl_database_configuration_filename)

    etl_database_configuration_file = \
        Files(
            absolute_path_string=etl_database_configuration_file_path)

    return \
        etl_database_configuration_file
