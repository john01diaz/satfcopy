import os
from nf_common_source.code.services.b_app_runner_service.b_app_runner import run_b_app
from nf_common_source.code.services.file_system_service.objects.files import Files
from nf_common_source.code.services.file_system_service.objects.folders import Folders
from nf_common_source.code.services.log_environment_utility_service.common_knowledge.environment_log_level_types import \
    EnvironmentLogLevelTypes
from sat_configuration_source.b_code.orchestrators.json_configurations_wrapper_orchestrator import \
    orchestrate_json_configurations_wrapper


if __name__ == '__main__':
    user_initials = \
        'JPr'

    b_root_folder_path = \
        os.path.join(
            'd:',
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

    etl_database_configuration_file_path = \
        os.path.join(
            configurations_folder_path,
            'process_table_configuration v0.02 AGu modified OXi+AMi_CPa_v5_run_processes.accdb')

    run_b_app(
        app_startup_method=orchestrate_json_configurations_wrapper,
        environment_log_level_type=EnvironmentLogLevelTypes.FILTERED,
        output_folder_prefix='STN_io_allocation',
        output_root_folder=Folders(absolute_path_string=configurations_folder_path),
        etl_database_configuration_file=Files(absolute_path_string=etl_database_configuration_file_path),
        etl_json_configuration_file_name='etl_run_configuration_full_RUN_NAME.json',
        etl_json_configuration_filtered_file_name='silver_10_s_IO_Catalogue_sql_01_00.json')
