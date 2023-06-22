from sat_workflow_source.b_code.etl_processes.common.global_flags import GlobalFlags
from sat_workflow_source.b_code.etl_processes_wrapper.objects.etl_processes_wrapper_configurations import \
    EtlProcessesWrapperConfigurations
from sat_workflow_source.a_startup.common.common_b_app_runner_standard_exemplar_configuration_getter import \
    get_common_b_app_runner_standard_exemplar_configuration


def get_b_app_runner_standard_exemplar_configuration_agu() \
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
                user_initials='AGu',
                configuration_json_filename='process_table_configuration v0.02 AGu modified OXi+AMi_CPa_v6_run_processes SQL gold 01 and 02.json',
                run_new_vs_original_comparison=True,
                output_folder_prefix='s_devicecatalogue lower case',
                output_folder_suffix=str(),
                main_wrapper_outputs_folder_name='main_wrapper_outputs')
    
    return \
        etl_processes_wrapper_configuration
