from nf_common_source.code.services.b_app_runner_service.b_app_runner import run_b_app
from nf_common_source.code.services.file_system_service.objects.folders import Folders
from nf_common_source.code.services.log_environment_utility_service.common_knowledge.environment_log_level_types import \
    EnvironmentLogLevelTypes

from sat_workflow_source.b_code.etl_processes.common.global_flags import GlobalFlags
from z_old_code.etl_processes.etl_layers.runners_v2.gold.gold_c07_loop_index_sql_01_00_v0_02_runners \
    import GoldC07LoopIndexSql0100Runners
from sat_workflow_source.b_code.etl_processes_wrapper.objects.etl_processes_wrapper_configurations import \
    EtlProcessesWrapperConfigurations
from sat_workflow_source.b_code.etl_processes_wrapper.orchestrators.etl_processes_wrapper_orchestrator import \
    orchestrate_etl_processes_wrapper

if __name__ == '__main__':
    output_root_folder = \
        Folders(
            absolute_path_string=r'C:\bWa\AGu\etl\main_wrapper_outputs')

    parquet_bronze_scripts_root_folder_path = \
        r'C:\bWa\AGu\etl\collect\blob\blob-temp-anusha_folder-sigraph_bronze_2023_04_19'

    parquet_silver_scripts_root_folder_path = \
        r'C:\bWa\AGu\etl\collect\blob\blob-temp-anusha_folder-sigraph_silver_2023_04_19'

    loader_gold_scripts_root_folder_path = \
        r'C:\bWa\AGu\etl\collect\loader_files\Gold_Scripts'

    # HardcodedConfigurations.loop_picklist_file_path = \
    #     r'C:\bWa\AGu\etl\collect\blob\blob-temp-anusha_folder-sigraph_crosswalks_db_2023_04_20\sigraph_crosswalks\Loop_List_Cross_walk.xlsx'

    # [0]=root_folder_path, [1]=storage type, [2]=parquet folder/xlsx file name, [3]=table name, [4]=registry type, [5]=id column names , [6]=xlsx sheet name, [7]=column names to set to default, [8]=Enum for filtered column names
    file_configuration_list = \
        [
            # [parquet_bronze_scripts_root_folder_path, 'parquet', 'sigraph_bronze', 'CS_Layer_Loop_Loop_elements', \
            #  'input', None, '', None, CS_Layer_Loop_Loop_elements_FilteredForInstrumentTypeCatalogueInitialCase],
            # [parquet_bronze_scripts_root_folder_path, 'parquet', 'sigraph_bronze', 'CS_Layer_Loop_Loop_elements', \
            #  'input', None, '', None, CS_Layer_Loop_Loop_elements_FilteredForProjectVoltagesParquet],
            # [parquet_silver_scripts_root_folder_path, 'parquet', 'sigraph_silver', 'S_Itemfunction', 'input', None, ''],
            # [parquet_silver_scripts_root_folder_path, 'parquet', 'sigraph_silver', 'S_Item_Function_Model', 'input', None, ''],
            # [parquet_silver_scripts_root_folder_path, 'parquet', 'sigraph_silver', 'S_DeviceCatalogue', 'original_output', ['object_identifier'], '', ['description']],
            # [loader_gold_scripts_root_folder_path, 'xlsx', '4_Device_Catalogue.xlsx', 'Device_Catalogue', 'original_output', None, 'export (22)', ['description']]
            # [parquet_silver_scripts_root_folder_path, 'parquet', 'sigraph_silver',
            # 'S_CableCatalogue', 'input', None, ''],
            # [parquet_silver_scripts_root_folder_path, 'parquet', 'sigraph_silver',
            # 'S_CableCoreCatalogue', 'input', None, ''],
            # [parquet_silver_scripts_root_folder_path, 'parquet', 'sigraph_silver',
            # 'S_CableCatalogueNumber_Master', 'input', None, ''],
            # [parquet_silver_scripts_root_folder_path, 'parquet', 'sigraph_silver',
            # 'S_DeviceCatalogue', 'input', None, ''],
            # [parquet_silver_scripts_root_folder_path, 'parquet', 'sigraph_silver',
            # 'S_Field_Device_Catalogue', 'input', None, ''],
            # [parquet_silver_scripts_root_folder_path, 'parquet', 'sigraph_silver',
            # 'S_IO_Catalogue', 'input', None, ''],
            [parquet_silver_scripts_root_folder_path, 'parquet', 'sigraph_silver',
            'S_Loop_Index', 'input', None, ''],
            # [parquet_silver_scripts_root_folder_path, 'parquet', 'sigraph_silver',
            # 'S_Instrument_Index', 'input', None, ''],
            # [parquet_silver_scripts_root_folder_path, 'parquet', 'sigraph_silver',
            # 'S_Major_Equipments', 'input', None, ''],
            # [parquet_silver_scripts_root_folder_path, 'parquet', 'sigraph_silver',
            # 'S_Component', 'input', None, ''],
            # [parquet_silver_scripts_root_folder_path, 'parquet', 'sigraph_silver',
            # 'S_CableSchedule', 'input', None, ''],
            # # [parquet_silver_scripts_root_folder_path, 'parquet', 'sigraph_silver',
            # 'S_Terminals', 'input', None, ''],
            # # [parquet_silver_scripts_root_folder_path, 'parquet', 'sigraph_silver',
            # 'S_Terminations', 'input', None, ''],
            # [parquet_silver_scripts_root_folder_path, 'parquet', 'sigraph_silver',
            # 'S_Internal_Wiring', 'input', None, ''],
            # [parquet_silver_scripts_root_folder_path, 'parquet', 'sigraph_silver',
            # 'S_IO_Allocations', 'input', None, ''],
    
            # [loader_gold_scripts_root_folder_path, 'xlsx', '1_Project_Voltages.xlsx', 'Project_Voltages',
            # 'original_output', None, 'export (18)'],
            # [loader_gold_scripts_root_folder_path, 'xlsx', '2_Instrument_Type_Catalogue.xlsx',
            #  'Instrument_Type_Catalogue', 'original_output', None, 'export (19)'],
            # [loader_gold_scripts_root_folder_path, 'xlsx', '3_Cable_Catalogue.xlsx', 'Cable_Catalogue',
            # 'original_output', None, 'export (20)'],
            # [loader_gold_scripts_root_folder_path, 'xlsx', '3_Cable_Core_Catalogue.xlsx', 'Cable_Core_Catalogue',
            # 'original_output', None, 'export (21)'],
            # [loader_gold_scripts_root_folder_path, 'xlsx', '4_Device_Catalogue.xlsx', 'Device_Catalogue',
            # 'original_output', None, 'export (22)'],
            # [loader_gold_scripts_root_folder_path, 'xlsx', '5_Field_Device_Catalogue.xlsx', 'Field_Device_Catalogue',
            # 'original_output', None, 'export (23)'],
            # [loader_gold_scripts_root_folder_path, 'xlsx', '6_IO_Catalogue.xlsx', 'IO_Catalogue', 'original_output',
            # None, 'export (24)'],
            [loader_gold_scripts_root_folder_path, 'xlsx', '7_Loop_Index.xlsx', 'Loop_Index', 'original_output', None,
            'export (25)'],
            # [loader_gold_scripts_root_folder_path, 'xlsx', '8_Instrument_Index.xlsx', 'Instrument_Index',
            # 'original_output', None, 'export (26)'],
            # [loader_gold_scripts_root_folder_path, 'xlsx', '9_Major_Equipments.xlsx', 'Major_Equipments',
            # 'original_output', None, 'export (21)'],
            # [loader_gold_scripts_root_folder_path, 'xlsx', '10_Component.xlsx', 'Component', 'original_output', None,
            # 'export (27)'],
            # [loader_gold_scripts_root_folder_path, 'xlsx', '11_Cable_Schedule.xlsx', 'Cable_Schedule', 'original_output',
            # None, 'export (28)'],
            # # [loader_gold_scripts_root_folder_path, 'xlsx', '12_Terminals.xlsx', 'Terminals', 'original_output', None,
            # 'export (29)'],
            # # [loader_gold_scripts_root_folder_path, 'xlsx', '13_Terminations.xlsx', 'Terminations', 'original_output',
            # None, 'export (31)'],
            # [loader_gold_scripts_root_folder_path, 'xlsx', '14_Interal_Wiring.xlsx', 'Interal_Wiring', 'original_output',
            #  None, 'export (19)'],
            # [loader_gold_scripts_root_folder_path, 'xlsx', '15_IO_Allocations.xlsx', 'IO_Allocations', 'original_output',
            # None, 'export (20)']
        ]

    process_configuration_list = \
        [
            # run_gold_c01_project_voltages_sql_01_00,
            # run_gold_c02_instrument_type_catalogue_sql_01_00,
            # run_gold_c03_cable_catalogue_sql_01_00,
            # run_gold_c03_cable_core_catalogue_sql_01_00,
            # run_gold_c04_device_catalogue_sql_01_00,
            # run_gold_c05_field_device_catalogue_sql_01_00,
            # run_gold_c06_io_catalogue_sql_01_00,
            # run_gold_c07_loop_index_sql_01_00_v0_02,
            GoldC07LoopIndexSql0100Runners,
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

    GlobalFlags.RUN_PROCESS_FILES = \
        True

    GlobalFlags.RUN_BIEIZE = \
        False

    GlobalFlags.RUN_BIEIZE_SANITY_CHECK = \
        False

    GlobalFlags.RUN_BIEIZE_COMPARISON = \
        False

    run_b_app(
        app_startup_method=orchestrate_etl_processes_wrapper,
        environment_log_level_type=EnvironmentLogLevelTypes.FILTERED,
        output_folder_prefix='etl_processes_wrapper',
        output_folder_suffix='Gold_07_Loop_Index',
        output_root_folder=output_root_folder,
        etl_processes_wrapper_configuration=etl_processes_wrapper_configuration)
