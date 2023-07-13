import os
from nf_common_source.code.services.file_system_service.objects.folders import Folders
from sat_workflow_source.b_code.etl_processes.common.global_flags import GlobalFlags
from z_old_code.etl_processes.etl_layers.runners_v2.silver.silver_c08_s_device_catalogue_sql_01_00_runners import \
    SilverC08DeviceCatalogueSql0100Runners
from sat_workflow_source.b_code.etl_processes_wrapper.objects.etl_processes_wrapper_configurations import \
    EtlProcessesWrapperConfigurations
from z_sandpit.common.hardcoded_configurations import HardcodedConfigurations


def get_b_app_runner_standard_exemplar_configuration_dza() \
        -> EtlProcessesWrapperConfigurations:
    user_initials = \
        'DZa'
    
    # NOTE: default is True
    GlobalFlags.RUN_PROCESS_FILES = \
        True
    
    # NOTE: default is True
    GlobalFlags.RUN_BIEIZE = \
        True
    
    # NOTE: default is True
    GlobalFlags.RUN_BIEIZE_SANITY_CHECK = \
        False
    
    # NOTE: default is True
    GlobalFlags.RUN_BIEIZE_COMPARISON = \
        True
    
    # NOTE: typically use the output file name
    output_folder_prefix = \
        's_device_catalogue'
    
    output_folder_suffix = \
        'SILVER'

    main_wrapper_outputs_folder_name = \
        'main_wrapper_outputs'

    b_root_folder_path = \
        os.path.join(
            'c:',
            os.sep,
            'bWa')

    etl_root_folder_path = \
        os.path.join(
            b_root_folder_path,
            user_initials,
            'etl')
    
    output_root_folder = \
        Folders(
            absolute_path_string=os.path.join(
                etl_root_folder_path,
                main_wrapper_outputs_folder_name))
    
    collect_blob_folder_path = \
        os.path.join(
            etl_root_folder_path,
            'collect',
            'blob')
    
    parquet_bronze_root_folder_path = \
        os.path.join(
            collect_blob_folder_path,
            'blob-temp-anusha_folder-sigraph_bronze_2023_05_21_1500s')
    
    parquet_silver_root_folder_path = \
        os.path.join(
            collect_blob_folder_path,
            'blob-temp-anusha_folder-sigraph_silver_2023_05_21_1500')
    
    loader_gold_scripts_root_folder_path = \
        os.path.join(
            etl_root_folder_path,
            'collect',
            'loader_files',
            'Gold_Scripts')
    
    HardcodedConfigurations.loop_picklist_file_path = \
        os.path.join(
            collect_blob_folder_path,
            'blob-temp-anusha_folder-sigraph_crosswalks_db_2023_05_21_1500',
            'sigraph_crosswalks,',
            'Loop_List_Cross_walk.xlsx')
    
    # [0]=root_folder_path, [1]=storage type, [2]=parquet folder/xlsx file name, [3]=table name, [4]=registry type,
    # [5]=id column names , [6]=xlsx sheet name, [7]=column names to set to default, [8]=Enum for filtered column names
    file_configuration_list = \
        [
            ##### BRONZE #####
            # [parquet_bronze_root_folder_path, 'parquet', 'sigraph_bronze', 'CS_Layer_Loop_Loop_elements', 'input', None, ''],
            ##### SILVER #####
            # [parquet_silver_root_folder_path, 'parquet', 'sigraph_silver', 'S_CableCatalogue', 'input', None, ''],
            # [parquet_silver_root_folder_path, 'parquet', 'sigraph_silver', 'S_CableCoreCatalogue', 'input', None, ''],
            # [parquet_silver_root_folder_path, 'parquet', 'sigraph_silver', 'S_CableCatalogueNumber_Master', 'original_output', ['cable_object_identifier'], ''],
            [parquet_silver_root_folder_path, 'parquet', 'sigraph_silver', 'S_ItemFunction', 'input', None, ''],
            [parquet_silver_root_folder_path, 'parquet', 'sigraph_silver', 'S_Item_Function_Model', 'input', None, ''],
            [parquet_silver_root_folder_path, 'parquet', 'sigraph_silver', 'S_DeviceCatalogue', 'original_output', None, ''],
            # [parquet_silver_root_folder_path, 'parquet', 'sigraph_silver', 'S_Connection', 'input', None, ''],
            # [parquet_silver_root_folder_path, 'parquet', 'sigraph_silver', 'S_Terminals', 'input', None, ''],
            # [parquet_silver_root_folder_path, 'parquet', 'sigraph_silver', 'S_Internal_Wiring', 'original_output', None, ''],
            # [parquet_silver_root_folder_path, 'parquet', 'sigraph_silver', 'S_Field_Device_Catalogue', 'input', None, ''],
            # [parquet_silver_root_folder_path, 'parquet', 'sigraph_silver', 'S_IO_Catalogue', 'input', None, ''],
            # [parquet_silver_root_folder_path, 'parquet', 'sigraph_silver', 'S_Loop_Index', 'input', None, ''],
            # [parquet_silver_root_folder_path, 'parquet', 'sigraph_silver', 'S_Instrument_Index', 'input', None, ''],
            # [parquet_silver_root_folder_path, 'parquet', 'sigraph_silver', 'S_Major_Equipments', 'input', None, ''],
            # [parquet_silver_root_folder_path, 'parquet', 'sigraph_silver', 'S_Component', 'input', None, ''],
            # [parquet_silver_root_folder_path, 'parquet', 'sigraph_silver', 'S_CableSchedule', 'input', None, ''],
            # [parquet_silver_root_folder_path, 'parquet', 'sigraph_silver', 'S_Terminations', 'input', None, ''],
            # [parquet_silver_root_folder_path, 'parquet', 'sigraph_silver', 'S_IO_Allocations', 'input', None, ''],
            ##### GOLD #####
            # [loader_gold_scripts_root_folder_path, 'xlsx', '1_Project_Voltages.xlsx', 'Project_Voltages', 'original_output', None, 'export (18)'],
            # [loader_gold_scripts_root_folder_path, 'xlsx', '2_Instrument_Type_Catalogue.xlsx', 'Instrument_Type_Catalogue', 'original_output', None, 'export (19)'],
            # [loader_gold_scripts_root_folder_path, 'xlsx', '3_Cable_Catalogue.xlsx', 'Cable_Catalogue', 'original_output', None, 'export (20)'],
            # [loader_gold_scripts_root_folder_path, 'xlsx', '3_Cable_Core_Catalogue.xlsx', 'Cable_Core_Catalogue', 'original_output', None, 'export (21)'],
            # [loader_gold_scripts_root_folder_path, 'xlsx', '4_Device_Catalogue.xlsx', 'Device_Catalogue', 'original_output', None, 'export (22)'],
            # [loader_gold_scripts_root_folder_path, 'xlsx', '5_Field_Device_Catalogue.xlsx', 'Field_Device_Catalogue', 'original_output', None, 'export (23)'],
            # [loader_gold_scripts_root_folder_path, 'xlsx', '6_IO_Catalogue.xlsx', 'IO_Catalogue', 'original_output', None, 'export (24)'],
            # [loader_gold_scripts_root_folder_path, 'xlsx', '7_Loop_Index.xlsx', 'Loop_Index', 'original_output', None, 'export (25)'],
            # [loader_gold_scripts_root_folder_path, 'xlsx', '8_Instrument_Index.xlsx', 'Instrument_Index', 'original_output', None, 'export (26)'],
            # [loader_gold_scripts_root_folder_path, 'xlsx', '9_Major_Equipments.xlsx', 'Major_Equipments', 'original_output', None, 'export (21)'],
            # [loader_gold_scripts_root_folder_path, 'xlsx', '10_Component.xlsx', 'Component', 'original_output', None, 'export (27)'],
            # [loader_gold_scripts_root_folder_path, 'xlsx', '11_Cable_Schedule.xlsx', 'Cable_Schedule', 'original_output', None, 'export (28)'],
            # [loader_gold_scripts_root_folder_path, 'xlsx', '12_Terminals.xlsx', 'Terminals', 'original_output', None, 'export (29)'],
            # [loader_gold_scripts_root_folder_path, 'xlsx', '13_Terminations.xlsx', 'Terminations', 'original_output', None, 'export (31)'],
            # [loader_gold_scripts_root_folder_path, 'xlsx', '14_Interal_Wiring.xlsx', 'Interal_Wiring', 'original_output', None, 'export (19)'],
            # [loader_gold_scripts_root_folder_path, 'xlsx', '15_IO_Allocations.xlsx', 'IO_Allocations', 'original_output', None, 'export (20)']
        ]

    process_configuration_list = \
        [
            ##### SILVER #####
            # run_silver_06_s_cablecatalogue_number_generation_sql_01_00,
            # run_silver_08_s_device_catalogue_sql_01_00,
            # run_silver_18_s_internal_wiring_sql_01_00,
            SilverC08DeviceCatalogueSql0100Runners
            ##### GOLD #####
            # run_gold_c01_project_voltages_sql_01_00,
            # run_gold_c02_instrument_type_catalogue_sql_01_00,
            # run_gold_c03_cable_catalogue_sql_01_00,
            # run_gold_c03_cable_core_catalogue_sql_01_00,
            # run_gold_c04_device_catalogue_sql_01_00,
            # run_gold_c05_field_device_catalogue_sql_01_00
            # run_gold_c06_io_catalogue_sql_01_00
            # run_gold_c07_loop_index_sql_01_00_v0_02,
            # run_gold_c08_instrument_index_sql_01_00,
            # run_gold_c09_major_equipment_sql_01_00
            # run_gold_c10_component_sql_01_00,
            # run_gold_c11_cable_schedule_sql_01_00_v0_02,
            # run_gold_c12_terminals_sql_01_00,
            # run_gold_c13_terminations_sql_01_00,
            # run_gold_c14_internal_wiring_sql_01_00
            # run_gold_c15_io_allocations_sql_01_00
        ]
    
    etl_processes_wrapper_configuration = \
        EtlProcessesWrapperConfigurations(
            file_configuration_list=file_configuration_list,
            process_configuration_list=process_configuration_list,
            output_root_folder=output_root_folder,
            output_folder_prefix=output_folder_prefix,
            output_folder_suffix=output_folder_suffix)
    
    return \
        etl_processes_wrapper_configuration
