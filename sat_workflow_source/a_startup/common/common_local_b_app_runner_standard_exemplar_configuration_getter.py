import os
from nf_common_source.code.services.file_system_service.objects.files import Files
from nf_common_source.code.services.file_system_service.objects.folders import Folders
from sat_workflow_source.b_code.etl_processes_wrapper.objects.etl_processes_wrapper_configurations import \
    EtlProcessesWrapperConfigurations


def get_common_local_b_app_runner_standard_exemplar_configuration(
        drive_name: str,
        user_initials: str,
        configuration_json_relative_file_path: str,
        output_folder_prefix: str,
        output_folder_suffix: str,
        main_wrapper_outputs_folder_name: str) \
        -> EtlProcessesWrapperConfigurations:
    etl_root_folder_path = \
        __get_etl_root_folder_path(
            drive_name=drive_name,
            user_initials=user_initials)

    etl_json_configuration_file = \
        __get_etl_json_configuration_file(
            etl_root_folder_path=etl_root_folder_path,
            configuration_json_relative_file_path=configuration_json_relative_file_path)

    etl_sources_root_folder = \
        __get_etl_sources_root_folder(
            etl_root_folder_path=etl_root_folder_path)

    output_root_folder = \
        __get_output_root_folder(
            etl_root_folder_path=etl_root_folder_path,
            main_wrapper_outputs_folder_name=main_wrapper_outputs_folder_name)

    etl_processes_wrapper_configuration = \
        EtlProcessesWrapperConfigurations(
            etl_json_configuration_file=etl_json_configuration_file,
            etl_sources_root_folder=etl_sources_root_folder,
            output_root_folder=output_root_folder,
            output_folder_prefix=output_folder_prefix,
            output_folder_suffix=output_folder_suffix)
    
    return \
        etl_processes_wrapper_configuration


def __get_etl_root_folder_path(
        drive_name: str,
        user_initials: str) \
        -> str:
    b_root_folder_path = \
        os.path.join(
            drive_name,
            os.sep,
            'bWa')

    etl_root_folder_path = \
        os.path.join(
            b_root_folder_path,
            user_initials,
            'etl')

    return \
        etl_root_folder_path


def __get_etl_json_configuration_file(
        etl_root_folder_path: str,
        configuration_json_relative_file_path: str) \
        -> Files:
    etl_json_configuration_file_path = \
        os.path.join(
            etl_root_folder_path,
            'configurations',
            configuration_json_relative_file_path)

    etl_json_configuration_file = \
        Files(
            absolute_path_string=etl_json_configuration_file_path)

    return \
        etl_json_configuration_file


def __get_etl_sources_root_folder(
        etl_root_folder_path: str) \
        -> Folders:
    etl_sources_root_folder_path = \
        os.path.join(
            etl_root_folder_path,
            'collect')

    etl_sources_root_folder = \
        Folders(
            absolute_path_string=etl_sources_root_folder_path)

    return \
        etl_sources_root_folder


def __get_output_root_folder(
        etl_root_folder_path: str,
        main_wrapper_outputs_folder_name: str) \
        -> Folders:
    output_root_folder_path = \
        os.path.join(
            etl_root_folder_path,
            main_wrapper_outputs_folder_name)

    output_root_folder = \
        Folders(
            absolute_path_string=output_root_folder_path)

    return \
        output_root_folder
