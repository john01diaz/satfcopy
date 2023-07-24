from nf_common_source.code.services.b_app_runner_service.b_app_runner import run_b_app
from nf_common_source.code.services.file_system_service.objects.folders import Folders
from nf_common_source.code.services.log_environment_utility_service.common_knowledge.environment_log_level_types import \
    EnvironmentLogLevelTypes

from z_old_code.etl_processes.etl_layers.runners_v2.silver import \
    run_silver_06_s_cablecatalogue_number_generation_sql_01_00
from sat_workflow_source.b_code.etl_processes_wrapper.objects.etl_processes_wrapper_configurations import \
    EtlProcessesWrapperConfigurations
from sat_workflow_source.b_code.etl_processes_wrapper.orchestrators.etl_processes_wrapper_orchestrator import \
    orchestrate_etl_processes_wrapper

if __name__ == '__main__':
    input_root_folder_parquet = \
        r'C:\bWa\AGu\etl\collect\blob\blob-temp-anusha_folder-sigraph_silver_2023_04_19'

    input_root_folder_gold_scripts = \
        r'C:\bWa\AGu\etl\collect\loader_files\Gold_Scripts'
    
    output_root_folder = \
        Folders(
            absolute_path_string=r'C:\bWa\AGu\etl\main_wrapper_outputs')

    #TODO This needs handle multiple roles for the same file in the future.
    file_configuration_list = \
        [

            # [input_root_folder_parquet, 'parquet', 'sigraph_silver', 'S_Itemfunction', 'input', None, ''],
            # [input_root_folder_parquet, 'parquet', 'sigraph_silver', 'S_Item_Function_Model', 'input', None, ''],
            # [input_root_folder_parquet, 'parquet', 'sigraph_silver', 'S_Connection', 'input', None, ''],

            # TODO: This are silver requirements
            # [input_root_folder_parquet, 'parquet', 'sigraph_bronze', 'CS_Layer_Loop_Loop_elements', 'input', None, ''],

            [input_root_folder_parquet, 'parquet', 'sigraph_silver', 'S_CableCatalogue', 'input', None, ''],
            [input_root_folder_parquet, 'parquet', 'sigraph_silver', 'S_CableCoreCatalogue', 'input', None, ''],
            [input_root_folder_parquet, 'parquet', 'sigraph_silver', 'S_CableCatalogueNumber_Master', 'original_output', None,''],
            #
            # [input_root_folder_parquet, 'parquet', 'sigraph_silver', 'S_Itemfunction', 'input', None, ''],
            # [input_root_folder_parquet, 'parquet', 'sigraph_silver', 'S_Item_Function_Model', 'input', None, ''],
            # [input_root_folder_parquet, 'parquet', 'sigraph_silver', 'S_DeviceCatalogue', 'original_output', None, ''],

            # [input_root_folder_parquet, 'parquet', 'sigraph_silver', 'S_Field_Device_Catalogue', 'input', None, ''],
            # [input_root_folder_parquet, 'parquet', 'sigraph_silver', 'S_IO_Catalogue', 'input', None, ''],
            # [input_root_folder_parquet, 'parquet', 'sigraph_silver', 'S_Loop_Index', 'input', None, ''],
            # [input_root_folder_parquet, 'parquet', 'sigraph_silver', 'S_Instrument_Index', 'input', None, ''],
            # [input_root_folder_parquet, 'parquet', 'sigraph_silver', 'S_Major_Equipments', 'input', None, ''],
            # [input_root_folder_parquet, 'parquet', 'sigraph_silver', 'S_Component', 'input', None, ''],
            # [input_root_folder_parquet, 'parquet', 'sigraph_silver', 'S_CableSchedule', 'input', None, ''],
            # [input_root_folder_parquet, 'parquet', 'sigraph_silver', 'S_Terminals', 'input', None, ''],
            # [input_root_folder_parquet, 'parquet', 'sigraph_silver', 'S_Terminations', 'input', None, ''],
            # [input_root_folder_parquet, 'parquet', 'sigraph_silver', 'S_Internal_Wiring', 'input', None, ''],
            # [input_root_folder_parquet, 'parquet', 'sigraph_silver', 'S_IO_Allocations', 'input', None, ''],
            #
            # [input_root_folder_parquet, 'parquet', 'sigraph_silver', 'S_Internal_Wiring', 'original_output', None, ''],

            # TODO: This are gold requirements
        ]

    process_configuration_list = \
        [
            run_silver_06_s_cablecatalogue_number_generation_sql_01_00,
            # run_silver_08_s_device_catalogue_sql_01_00,
            # run_silver_18_s_internal_wiring_sql_01_00,
            # # run_silver_c11_s_loop_index_sql_01_00,
        ]

    etl_processes_wrapper_configuration = \
        EtlProcessesWrapperConfigurations(
            file_configuration_list=file_configuration_list,
            process_configuration_list=process_configuration_list)

    run_b_app(
        app_startup_method=orchestrate_etl_processes_wrapper,
        environment_log_level_type=EnvironmentLogLevelTypes.FILTERED,
        output_folder_prefix='silver_tranche_1_etl_processes_wrapper',
        output_root_folder=output_root_folder,
        etl_processes_wrapper_configuration=etl_processes_wrapper_configuration)
