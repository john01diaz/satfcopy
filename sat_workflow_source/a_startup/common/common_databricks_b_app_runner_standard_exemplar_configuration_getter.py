import os
from nf_common_source.code.services.file_system_service.objects.files import Files
from nf_common_source.code.services.file_system_service.objects.folders import Folders
from sat_workflow_source.b_code.etl_processes_wrapper.objects.etl_processes_wrapper_configurations import \
    EtlProcessesWrapperConfigurations


def get_common_databricks_b_app_runner_standard_exemplar_configuration(
        configuration_json_filename: str,
        output_folder_prefix: str,
        output_folder_suffix: str,
        main_wrapper_outputs_folder_name: str) \
        -> EtlProcessesWrapperConfigurations:
    bclearer_root_folder_path = \
        os.path.join(
            os.sep,
            'mnt',
            'bclearer')

    etl_sources_root_folder_path = \
        os.path.join(
            bclearer_root_folder_path,
            'collect',
            'rheinland',
            'sigraph',
            'parquets_for_etl')

    etl_sources_root_folder = \
        Folders(
            absolute_path_string=etl_sources_root_folder_path)

    etl_running_root_folder_path = \
        os.path.join(
            bclearer_root_folder_path,
            'temp',
            'alex_sandpit')

    etl_json_configuration_file_path = \
        os.path.join(
            etl_running_root_folder_path,
            'configurations',
            configuration_json_filename)

    etl_json_configuration_file = \
        Files(
            absolute_path_string=etl_json_configuration_file_path)
    
    output_root_folder = \
        Folders(
            absolute_path_string=os.path.join(
                etl_running_root_folder_path,
                main_wrapper_outputs_folder_name))

    etl_processes_wrapper_configuration = \
        EtlProcessesWrapperConfigurations(
            etl_json_configuration_file=etl_json_configuration_file,
            etl_sources_root_folder=etl_sources_root_folder,
            output_root_folder=output_root_folder,
            output_folder_prefix=output_folder_prefix,
            output_folder_suffix=output_folder_suffix)
    
    return \
        etl_processes_wrapper_configuration
