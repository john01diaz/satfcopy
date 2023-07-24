import os
from nf_common_source.code.services.b_app_runner_service.b_app_runner import run_b_app
from nf_common_source.code.services.file_system_service.objects.folders import Folders
from nf_common_source.code.services.log_environment_utility_service.common_knowledge.environment_log_level_types import \
    EnvironmentLogLevelTypes

from sat_etl_script_source.b_code.orchestrators.shell_etl_files_converter_orchestrator import \
    orchestrate_shell_etl_files_converter

if __name__ == '__main__':
    collection_name = \
        'ETL_Scripts_20230627'

    input_root_folder = \
        Folders(
            absolute_path_string=r'/Users/terraire/bWa/DZa/antlr_ws/inputs/master'
                                 + os.sep + collection_name + os.sep + r'01_Instrumentation_ETL_Scripts')

    output_root_folder = \
        Folders(absolute_path_string=r'/Users/terraire/bWa/DZa/antlr_ws/inputs/outputs')

    run_b_app(
        app_startup_method=orchestrate_shell_etl_files_converter,
        environment_log_level_type=EnvironmentLogLevelTypes.FILTERED,
        output_folder_prefix=collection_name,
        output_root_folder=output_root_folder,
        input_root_folder=input_root_folder)
