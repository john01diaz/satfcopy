from nf_common_source.code.services.b_app_runner_service.b_app_runner import run_b_app
from nf_common_source.code.services.file_system_service.objects.folders import Folders
from nf_common_source.code.services.log_environment_utility_service.common_knowledge.environment_log_level_types import \
    EnvironmentLogLevelTypes

from sat_etl_script_source.b_code.orchestrators.shell_etl_files_converter_orchestrator import \
    orchestrate_shell_etl_files_converter

if __name__ == '__main__':
    collection_name = \
        'ETL_Scripts_20230627'

    path = \
        r'C:\bWa\AGu\etl\collect\ETL_Scripts_20230627_boro_fixed\01_Instrumentation_ETL_Scripts'

    input_root_folder = \
        Folders(
            absolute_path_string=path)

    output_root_folder = \
        Folders(
            absolute_path_string=r'C:\bWa\AGu\etl\file_conversion_outputs')

    run_b_app(
        app_startup_method=orchestrate_shell_etl_files_converter,
        environment_log_level_type=EnvironmentLogLevelTypes.FILTERED,
        output_folder_prefix=collection_name,
        output_root_folder=output_root_folder,
        input_root_folder=input_root_folder)
