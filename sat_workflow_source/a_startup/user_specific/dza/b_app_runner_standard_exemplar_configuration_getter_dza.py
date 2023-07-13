from sat_workflow_source.a_startup.common.common_local_b_app_runner_standard_exemplar_configuration_getter import \
    get_common_local_b_app_runner_standard_exemplar_configuration
from sat_workflow_source.b_code.etl_processes.common.global_flags import GlobalFlags
from sat_workflow_source.b_code.etl_processes_wrapper.objects.etl_processes_wrapper_configurations import \
    EtlProcessesWrapperConfigurations


def get_sat_workflow_b_app_runner_configuration_dza() \
        -> EtlProcessesWrapperConfigurations:
    GlobalFlags.RUN_PROCESS_FILES = \
        True

    GlobalFlags.RUN_BIEIZE = \
        True

    GlobalFlags.RUN_BIEIZE_SANITY_CHECK = \
        False

    GlobalFlags.RUN_BIEIZE_COMPARISON = \
        True

    # etl_run_configuration_filtered_stn_device_catalogue_generated.json
    # STN_Internal_Wiring_test.json

    etl_processes_wrapper_configuration = \
        get_common_local_b_app_runner_standard_exemplar_configuration(
            drive_name=r'/Users/terraire',
            user_initials='DZa',
            configuration_json_relative_file_path='Comparison_S_Cable_Catalogue_raw_to_clean_as_csvs/Comparison_S_Cable_Catalogue_raw_to_clean_as_csvs.json',
            output_folder_prefix='silver_s_cable_catalogue',
            output_folder_suffix='comparison_raw_to_clean_hotfix',
            main_wrapper_outputs_folder_name='main_wrapper_outputs')
    
    return \
        etl_processes_wrapper_configuration
