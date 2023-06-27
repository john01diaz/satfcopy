from sat_workflow_source.b_code.etl_processes.common.global_flags import GlobalFlags
from sat_workflow_source.b_code.etl_processes_wrapper.objects.etl_processes_wrapper_configurations import \
    EtlProcessesWrapperConfigurations
from sat_workflow_source.a_startup.common.common_b_app_runner_standard_exemplar_configuration_getter import \
    get_common_b_app_runner_standard_exemplar_configuration


def get_b_app_runner_standard_exemplar_configuration_oxi() \
        -> EtlProcessesWrapperConfigurations:
    GlobalFlags.RUN_PROCESS_FILES = \
        True

    GlobalFlags.RUN_BIEIZE = \
        True

    GlobalFlags.RUN_BIEIZE_SANITY_CHECK = \
        False

    GlobalFlags.RUN_BIEIZE_COMPARISON = \
        True

    etl_processes_wrapper_configuration = \
        get_common_b_app_runner_standard_exemplar_configuration(
            drive_name='C',
            user_initials='OXi',
            configuration_json_filename='filtered_v7_run_processes OXi2_multi_test_10_s_io_catalogue.json',
            # filtered_v7_run_processes OXi2_multi_test
            # filtered_v7_run_processes OXi2_multi_test_one_fragment_1
            # etl_run_configuration_filtered_database_names_v2
            run_new_vs_original_comparison=True,
            output_folder_prefix='multi_test_10_s_io_catalogue',
            output_folder_suffix=str(),
            main_wrapper_outputs_folder_name='main_wrapper_outputs')
    
    return \
        etl_processes_wrapper_configuration