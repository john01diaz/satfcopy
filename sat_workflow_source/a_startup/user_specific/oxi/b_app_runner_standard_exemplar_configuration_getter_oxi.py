from sat_workflow_source.b_code.etl_processes.common.global_flags import GlobalFlags
from sat_workflow_source.b_code.etl_processes_wrapper.objects.etl_processes_wrapper_configurations import \
    EtlProcessesWrapperConfigurations
from sat_workflow_source.a_startup.common.common_local_b_app_runner_standard_exemplar_configuration_getter import \
    get_common_local_b_app_runner_standard_exemplar_configuration


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

    GlobalFlags.FILTER_TO_DATABASE_LIST = \
        False

    etl_processes_wrapper_configuration = \
        get_common_local_b_app_runner_standard_exemplar_configuration(
            drive_name='C',
            user_initials='OXi',
            configuration_json_relative_file_path='etl_run_configuration_filtered_17_s_terminitations_python_mod.json',
            # 'configuration_database v20230720 v0.01 AGu JDi Silver 17.json',
            # '01_UT_CableCatalogue.prompt.json - testing json for the pickle error',
            # filtered_gold_io_catalogue_excel_new_silver  - new gold sql + excel export
            # filtered_configuration_s_08_parquet_new_silver  -  new silver sql + parquet export
            # filtered_v7_run_processes_OXi2_multi_test_silver_04  -  Item function model generation - uses udf functions
            # filtered_v7_run_processes OXi2_multi_test_10_s_io_catalogue_new_silver  -  It now uses udf functions
            # etl_run_configuration_filtered_database_names_v2  -  database_names_v2_small_gold_test mixed python sql
            # etl_run_configuration_filtered_database_names_v2_device_catalogue_python
            output_folder_prefix='17_s_terminitations_john_error_test_python_mod',
            output_folder_suffix=str(),
            main_wrapper_outputs_folder_name='main_wrapper_outputs')
    
    return \
        etl_processes_wrapper_configuration
