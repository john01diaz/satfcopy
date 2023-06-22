from nf_common_source.code.services.b_app_runner_service.b_app_runner import run_b_app
from nf_common_source.code.services.file_system_service.objects.folders import Folders
from nf_common_source.code.services.log_environment_utility_service.common_knowledge.environment_log_level_types import \
    EnvironmentLogLevelTypes
from sat_workflow_source.b_code.etl_processes.common.global_flags import GlobalFlags
from z_old_code.etl_processes.etl_layers.runners_v2.gold import \
    run_gold_c04_device_catalogue_sql_01_00_using_new_outputs
from z_old_code.etl_processes.etl_layers.runners_v2.silver import \
    run_silver_08_s_device_catalogue_sql_01_00
from sat_workflow_source.b_code.etl_processes_wrapper.objects.etl_processes_wrapper_configurations import \
    EtlProcessesWrapperConfigurations
from sat_workflow_source.b_code.etl_processes_wrapper.orchestrators.etl_processes_wrapper_orchestrator import \
    orchestrate_etl_processes_wrapper
from z_sandpit.common.hardcoded_configurations import HardcodedConfigurations

if __name__ == '__main__':
    output_root_folder = \
        Folders(
            absolute_path_string=r'C:\bWa\OXi\etl\main_wrapper_outputs')

    parquet_bronze_scripts_root_folder_path = \
        r'C:\bWa\OXi\etl\collect\blob\blob-temp-anusha_folder-sigraph_bronze_2023_04_19'

    parquet_silver_scripts_root_folder_path = \
        r'C:\bWa\OXi\etl\collect\blob\blob-temp-anusha_folder-sigraph_silver_2023_04_19'

    loader_gold_scripts_root_folder_path = \
        r'C:\bWa\OXi\etl\collect\loader_files\Gold_Scripts'

    HardcodedConfigurations.loop_picklist_file_path = \
        r'C:\bWa\OXi\etl\collect\blob\blob-temp-anusha_folder-sigraph_crosswalks_db_2023_04_20\sigraph_crosswalks\Loop_List_Cross_walk.xlsx'

    # [0]=root_folder_path, [1]=storage type, [2]=parquet folder/xlsx file name, [3]=table name, [4]=registry type, [5]=id column names , [6]=xlsx sheet name, [7]=column names to set to default, [8]=Enum for filtered column names
    file_configuration_list = \
        [
            # TODO: This are bronze requirements
            
            # TODO: This are silver requirements
            [parquet_silver_scripts_root_folder_path, 'parquet', 'sigraph_silver', 'S_Itemfunction', 'input', None, ''],
            [parquet_silver_scripts_root_folder_path, 'parquet', 'sigraph_silver', 'S_Item_Function_Model', 'input', None, ''],
            [parquet_silver_scripts_root_folder_path, 'parquet', 'sigraph_silver', 'S_DeviceCatalogue', 'original_output', ['object_identifier'], '', ['description']],

            # TODO: This are gold requirements
            [loader_gold_scripts_root_folder_path, 'xlsx', '4_Device_Catalogue.xlsx', 'Device_Catalogue', 'original_output', None, 'export (22)'],
        ]

    process_configuration_list = \
        [
            run_silver_08_s_device_catalogue_sql_01_00,
            run_gold_c04_device_catalogue_sql_01_00_using_new_outputs
        ]

    etl_processes_wrapper_configuration = \
        EtlProcessesWrapperConfigurations(
            file_configuration_list=file_configuration_list,
            process_configuration_list=process_configuration_list,
            run_new_vs_original_comparison=True)

    # GlobalFlags.RUN_PROCESS_FILES = \
    #     False

    GlobalFlags.RUN_BIEIZE = \
        True

    GlobalFlags.RUN_BIEIZE_SANITY_CHECK = \
        False

    GlobalFlags.RUN_BIEIZE_COMPARISON = \
        True

    run_b_app(
        app_startup_method=orchestrate_etl_processes_wrapper,
        environment_log_level_type=EnvironmentLogLevelTypes.FILTERED,
        output_folder_prefix='s_device_catalogue_STN',
        output_root_folder=output_root_folder,
        etl_processes_wrapper_configuration=etl_processes_wrapper_configuration)
