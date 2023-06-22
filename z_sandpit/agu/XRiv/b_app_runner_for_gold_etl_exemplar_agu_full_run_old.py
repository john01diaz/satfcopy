from nf_common_source.code.services.b_app_runner_service.b_app_runner import run_b_app
from nf_common_source.code.services.file_system_service.objects.folders import Folders
from nf_common_source.code.services.log_environment_utility_service.common_knowledge.environment_log_level_types \
    import \
    EnvironmentLogLevelTypes

from z_old_code.etl_processes.etl_layers.runners_v2.gold import run_gold_c02_instrument_type_catalogue_sql_01_00

from sat_workflow_source.b_code.etl_processes_wrapper.objects.etl_processes_wrapper_configurations import \
    EtlProcessesWrapperConfigurations
from sat_workflow_source.b_code.etl_processes_wrapper.orchestrators.etl_processes_wrapper_orchestrator import \
    orchestrate_etl_processes_wrapper

if __name__ == '__main__':
    input_root_folder_parquet = \
        r'C:\bWa\AGu\etl\collect\blob\test_AGu_gold'
    
    input_root_folder_gold_scripts = \
        r'C:\bWa\AGu\etl\collect\loader_files\Gold_Scripts_updated_for_test'
    
    output_root_folder = \
        Folders(
                absolute_path_string=r'C:\bWa\AGu\etl\main_wrapper_outputs')
    
    file_configuration_list = \
        [
            [input_root_folder_parquet, 'parquet', 'blob-temp-anusha_folder-sigraph_bronze_2023_04_19_CS_Layer_Loop_Loop_elements_one_file', 'CS_Layer_Loop_Loop_elements', 'input', None,
            ''],
            [parquet_bronze_scripts_root_folder_path, 'parquet', 'sigraph_bronze', 'CS_Layer_Loop_Loop_elements',
             'input', None, '', None, CSLayerLooploopElementsFilteredForProjectVoltages],
            # [input_root_folder_parquet, 'parquet', 'blob-temp-anusha_folder-sigraph_silver_2023_05_21_1500', 'S_CableCatalogue', 'input', None, ''],
            # [input_root_folder_parquet, 'parquet', 'blob-temp-anusha_folder-sigraph_silver_2023_05_21_1500', 'S_CableCoreCatalogue', 'input', None, ''],
            # [input_root_folder_parquet, 'parquet', 'blob-temp-anusha_folder-sigraph_silver_2023_05_21_1500', 'S_CableCatalogueNumber_Master', 'input',
            # None, ''],
            # [input_root_folder_parquet, 'parquet', 'blob-temp-anusha_folder-sigraph_silver_2023_05_21_1500', 'S_DeviceCatalogue', 'input', None, ''],
            # [input_root_folder_parquet, 'parquet', 'blob-temp-anusha_folder-sigraph_silver_2023_05_21_1500', 'S_Field_Device_Catalogue', 'input', None, ''],
            # [input_root_folder_parquet, 'parquet', 'blob-temp-anusha_folder-sigraph_silver_2023_05_21_1500', 'S_IO_Catalogue', 'input', None, ''],
            # [input_root_folder_parquet, 'parquet', 'blob-temp-anusha_folder-sigraph_silver_2023_05_21_1500', 'S_Loop_Index', 'input', None, ''],
            # [input_root_folder_parquet, 'parquet', 'blob-temp-anusha_folder-sigraph_silver_2023_05_21_1500', 'S_Instrument_Index', 'input', None, ''],
            # [input_root_folder_parquet, 'parquet', 'blob-temp-anusha_folder-sigraph_silver_2023_05_21_1500', 'S_Major_Equipments', 'input', None, ''],
            # [input_root_folder_parquet, 'parquet', 'blob-temp-anusha_folder-sigraph_silver_2023_05_21_1500', 'S_Component', 'input', None, ''],
            # [input_root_folder_parquet, 'parquet', 'blob-temp-anusha_folder-sigraph_silver_2023_05_21_1500', 'S_CableSchedule', 'input', None, ''],
            # # [input_root_folder_parquet, 'parquet', 'blob-temp-anusha_folder-sigraph_silver_2023_05_21_1500', 'S_Terminals', 'input', None, ''],
            # # [input_root_folder_parquet, 'parquet', 'blob-temp-anusha_folder-sigraph_silver_2023_05_21_1500', 'S_Terminations', 'input', None, ''],
            # [input_root_folder_parquet, 'parquet', 'blob-temp-anusha_folder-sigraph_silver_2023_05_21_1500', 'S_Internal_Wiring', 'input', None, ''],
            # [input_root_folder_parquet, 'parquet', 'blob-temp-anusha_folder-sigraph_silver_2023_05_21_1500', 'S_IO_Allocations', 'input', None, ''],
            
            # [input_root_folder_gold_scripts, 'xlsx', '1_Project_Voltages.xlsx', 'Project_Voltages',
            # 'original_output', None, 'export (18)'],
            [input_root_folder_gold_scripts, 'xlsx', '2_Instrument_Type_Catalogue.xlsx',
            'Instrument_Type_Catalogue', 'original_output', None, 'export (19)'],
            # [input_root_folder_gold_scripts, 'xlsx', '3_Cable_Catalogue.xlsx', 'Cable_Catalogue',
            # 'original_output', None, 'export (20)'],
            # [input_root_folder_gold_scripts, 'xlsx', '3_Cable_Core_Catalogue.xlsx', 'Cable_Core_Catalogue',
            # 'original_output', None, 'export (21)'],
            # [input_root_folder_gold_scripts, 'xlsx', '4_Device_Catalogue.xlsx', 'Device_Catalogue',
            # 'original_output', None, 'export (22)'],
            # [input_root_folder_gold_scripts, 'xlsx', '5_Field_Device_Catalogue.xlsx', 'Field_Device_Catalogue', 
            # 'original_output', None, 'export (23)'],
            # [input_root_folder_gold_scripts, 'xlsx', '6_IO_Catalogue.xlsx', 'IO_Catalogue', 'original_output', 
            # None, 'export (24)'],
            # [input_root_folder_gold_scripts, 'xlsx', '7_Loop_Index.xlsx', 'Loop_Index', 'original_output', None, 
            # 'export (25)'],
            # [input_root_folder_gold_scripts, 'xlsx', '8_Instrument_Index.xlsx', 'Instrument_Index', 
            # 'original_output', None, 'export (26)'],
            # [input_root_folder_gold_scripts, 'xlsx', '9_Major_Equipments.xlsx', 'Major_Equipments', 
            # 'original_output', None, 'export (21)'],
            # [input_root_folder_gold_scripts, 'xlsx', '10_Component.xlsx', 'Component', 'original_output', None, 
            # 'export (27)'],
            # [input_root_folder_gold_scripts, 'xlsx', '11_Cable_Schedule.xlsx', 'Cable_Schedule', 'original_output',
            # None, 'export (28)'],
            # # [input_root_folder_gold_scripts, 'xlsx', '12_Terminals.xlsx', 'Terminals', 'original_output', None, 
            # 'export (29)'],
            # # [input_root_folder_gold_scripts, 'xlsx', '13_Terminations.xlsx', 'Terminations', 'original_output', 
            # None, 'export (31)'],
            # [input_root_folder_gold_scripts, 'xlsx', '14_Interal_Wiring.xlsx', 'Interal_Wiring', 'original_output',
            #  None, 'export (19)'],
            # [input_root_folder_gold_scripts, 'xlsx', '15_IO_Allocations.xlsx', 'IO_Allocations', 'original_output',
            # None, 'export (20)']
            
            ]
    
    process_configuration_list = \
        [
            # run_gold_c01_project_voltages_sql_01_00,
            run_gold_c02_instrument_type_catalogue_sql_01_00,
            # run_gold_c03_cable_catalogue_sql_01_00,
            # run_gold_c03_cable_core_catalogue_sql_01_00,
            # run_gold_c04_device_catalogue_sql_01_00,
            # run_gold_c05_field_device_catalogue_sql_01_00,
            # run_gold_c06_io_catalogue_sql_01_00,
            # run_gold_c07_loop_index_sql_01_00_v0_02,
            # run_gold_c08_instrument_index_sql_01_00,
            # run_gold_c09_major_equipment_sql_01_00,
            # run_gold_c10_component_sql_01_00,
            # run_gold_c11_cable_schedule_sql_01_00_v0_02,
            # # run_gold_c12_terminals_sql_01_00,
            # # run_gold_c13_terminations_sql_01_00,
            # run_gold_c14_internal_wiring_sql_01_00
            # run_gold_c15_io_allocations_sql_01_00
            ]
    
    etl_processes_wrapper_configuration = \
        EtlProcessesWrapperConfigurations(
            file_configuration_list=file_configuration_list,
            process_configuration_list=process_configuration_list,
            run_new_vs_original_comparison=True)

    # GlobalFlags.RUN_PROCESS_FILES = \
    #     False
    #
    # GlobalFlags.RUN_BIEIZE = \
    #     False
    #
    # GlobalFlags.RUN_BIEIZE_SANITY_CHECK = \
    #     False
    #
    # GlobalFlags.RUN_BIEIZE_COMPARISON = \
    #     False

    run_b_app(
        app_startup_method=orchestrate_etl_processes_wrapper,
        environment_log_level_type=EnvironmentLogLevelTypes.FILTERED,
        output_folder_prefix='etl_processes_wrapper',
        output_folder_suffix='gold_c02_instrument_type_catalogue',
        output_root_folder=output_root_folder,
        etl_processes_wrapper_configuration=etl_processes_wrapper_configuration)

