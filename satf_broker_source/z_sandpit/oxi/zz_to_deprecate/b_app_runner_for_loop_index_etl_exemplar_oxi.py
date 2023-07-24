from nf_common_source.code.services.b_app_runner_service.b_app_runner import run_b_app
from nf_common_source.code.services.file_system_service.objects.folders import Folders
from nf_common_source.code.services.log_environment_utility_service.common_knowledge.environment_log_level_types import \
    EnvironmentLogLevelTypes
from z_old_code.etl_processes.etl_layers.runners_v2.gold import run_gold_c07_loop_index_sql_01_00_v0_02
from z_old_code.etl_processes.etl_layers.runners_v2.silver import \
    run_silver_c11_s_loop_index_sql_01_00
from sat_workflow_source.b_code.etl_processes_wrapper.objects.etl_processes_wrapper_configurations import \
    EtlProcessesWrapperConfigurations
from sat_workflow_source.b_code.etl_processes_wrapper.orchestrators.etl_processes_wrapper_orchestrator import \
    orchestrate_etl_processes_wrapper
from sat_workflow_source.b_code.model.hardcoded_configurations import HardcodedConfigurations

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

    # TODO: Add in 'parquet' to configuration line
    file_configuration_list = \
        [
            # TODO: This are bronze requirements
            
            # TODO: This are silver requirements
            [parquet_bronze_scripts_root_folder_path, 'parquet', 'sigraph_bronze', 'Layer', 'input', ['object_identifier']],
            [parquet_bronze_scripts_root_folder_path, 'parquet', 'sigraph_bronze', 'Loop', 'input', ['loop_object_identifier']],
            [r'C:\bWa\OXi\etl\collect\blob\blob-temp-anusha_folder-sigraph_crosswalks_db_2023_04_20', 'parquet', 'sigraph_crosswalks', 'PBS_Table', 'input', None],
            [parquet_silver_scripts_root_folder_path, 'parquet', 'sigraph_silver', 'S_Loop_Index', 'original_output', ['object_identifier']],

            # TODO: This are gold requirements
            # TODO: Stored twice - to be managed
            # [parquet_silver_scripts_root_folder_path, 'parquet', 'sigraph_silver', 'S_Loop_Index', 'input', ['object_identifier']],
            [loader_gold_scripts_root_folder_path, 'xlsx', '7_Loop_Index.xlsx', 'Loop_Index', 'original_output', None, 'export (25)'],

        ]

    process_configuration_list = \
        [
            run_silver_c11_s_loop_index_sql_01_00,
            run_gold_c07_loop_index_sql_01_00_v0_02
        ]

    etl_processes_wrapper_configuration = \
        EtlProcessesWrapperConfigurations(
            file_configuration_list=file_configuration_list,
            process_configuration_list=process_configuration_list)

    run_b_app(
        app_startup_method=orchestrate_etl_processes_wrapper,
        environment_log_level_type=EnvironmentLogLevelTypes.FILTERED,
        output_folder_prefix='loop_index_etl_processes_wrapper',
        output_root_folder=output_root_folder,
        etl_processes_wrapper_configuration=etl_processes_wrapper_configuration)
