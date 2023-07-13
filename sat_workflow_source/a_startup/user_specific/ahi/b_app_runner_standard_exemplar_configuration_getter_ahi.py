from sat_workflow_source.a_startup.common.common_databricks_b_app_runner_standard_exemplar_configuration_getter import \
    get_common_databricks_b_app_runner_standard_exemplar_configuration
from sat_workflow_source.b_code.etl_processes.common.global_flags import GlobalFlags
from sat_workflow_source.b_code.etl_processes_wrapper.objects.etl_processes_wrapper_configurations import \
    EtlProcessesWrapperConfigurations


def get_sat_workflow_b_app_runner_configuration_ahi() \
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
        False

    etl_processes_wrapper_configuration = \
        get_common_databricks_b_app_runner_standard_exemplar_configuration(
            configuration_json_filename=r'relative...json',
            output_folder_prefix='Comparison_S_Cable_Catalogue_raw_to_clean',
            output_folder_suffix='',
            main_wrapper_outputs_folder_name='comparison_outputs')

    return \
        etl_processes_wrapper_configuration
