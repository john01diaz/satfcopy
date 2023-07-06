from sat_workflow_source.b_code.etl_processes.common.global_flags import GlobalFlags
from sat_workflow_source.b_code.etl_processes_wrapper.objects.etl_processes_wrapper_configurations import \
    EtlProcessesWrapperConfigurations
from sat_workflow_source.a_startup.common.common_b_app_runner_standard_exemplar_configuration_getter import \
    get_common_b_app_runner_standard_exemplar_configuration


def get_sat_workflow_b_app_runner_configuration_ami() \
        -> EtlProcessesWrapperConfigurations:
    GlobalFlags.RUN_PROCESS_FILES = \
        True

    GlobalFlags.RUN_BIEIZE = \
        True

    GlobalFlags.RUN_BIEIZE_SANITY_CHECK = \
        True

    GlobalFlags.RUN_BIEIZE_COMPARISON = \
        True

    GlobalFlags.FILTER_TO_DATABASE_LIST = \
        True

    # etl_run_configuration_filtered_stn_device_catalogue_generated.json
    # STN_Internal_Wiring_test.json

    etl_processes_wrapper_configuration = \
        get_common_b_app_runner_standard_exemplar_configuration(
            drive_name='C',
            user_initials='AMi',
            configuration_json_filename=r'testing\satf_config_compare_cs_loop_spez_xml_v_aql_2cols.json',
            run_new_vs_original_comparison=True,
            output_folder_prefix='comparison',
            output_folder_suffix='xml_v_aql_2cols',
            main_wrapper_outputs_folder_name='main_wrapper_outputs')

    return \
        etl_processes_wrapper_configuration
