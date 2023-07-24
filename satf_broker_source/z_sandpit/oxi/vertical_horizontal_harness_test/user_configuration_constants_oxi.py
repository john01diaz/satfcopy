from nf_common_source.code.services.file_system_service.objects.folders import Folders
from sat_workflow_source.b_code.etl_processes.common.global_flags import GlobalFlags
from z_old_code.etl_processes.etl_layers.runners_v2.gold import \
    run_gold_c07_loop_index_sql_01_00_v0_02
from sat_workflow_source.b_code.etl_processes.etl_layers.python.runners import \
    run_gold_c03_cable_catalogue_sql_01_00_using_new_outputs
from z_old_code.etl_processes.etl_layers.runners_v2.gold import \
    run_gold_c03_cable_core_catalogue_sql_01_00_using_new_outputs
from z_old_code.etl_processes.etl_layers.runners_v2.gold import \
    run_gold_c04_device_catalogue_sql_01_00_using_new_outputs
from z_old_code.etl_processes.etl_layers.runners_v2.gold.using_new_outputs.gold_c13_terminations_sql_01_00_using_new_outputs_runner import \
    run_gold_c13_terminations_sql_01_00_using_new_outputs
from sat_workflow_source.b_code.etl_processes.etl_layers.python.runners import \
    run_gold_c14_internal_wiring_sql_01_00_using_new_outputs
from z_old_code.etl_processes.etl_layers.runners_v2.silver import \
    run_silver_06_s_cablecatalogue_number_generation_sql_01_00
from z_old_code.etl_processes.etl_layers.runners_v2.silver import \
    run_silver_08_s_device_catalogue_sql_01_00
from z_old_code.etl_processes.etl_layers.runners_v2.silver import \
    run_silver_c11_s_loop_index_sql_01_00
from z_old_code.etl_processes.etl_layers.runners_v2.silver import \
    run_silver_17_s_terminations_sql_01_00
from z_old_code.etl_processes.etl_layers.runners_v2.silver import \
    run_silver_18_s_internal_wiring_sql_01_00
from sat_workflow_source.b_code.model.hardcoded_configurations import HardcodedConfigurations


USER_OUTPUT_ROOT_FOLDER = \
    Folders(
        absolute_path_string=r'C:\bWa\OXi\etl\main_wrapper_outputs')

USER_PARQUET_BRONZE_SCRIPTS_ROOT_FOLDER_PATH = \
    r'C:\bWa\OXi\etl\collect\blob\blob-temp-anusha_folder-sigraph_bronze_2023_04_19'

USER_PARQUET_SILVER_SCRIPTS_ROOT_FOLDER_PATH = \
    r'C:\bWa\OXi\etl\collect\blob\blob-temp-anusha_folder-sigraph_silver_2023_04_19'

USER_LOADER_GOLD_SCRIPTS_ROOT_FOLDER_PATH = \
    r'C:\bWa\OXi\etl\collect\loader_files\Gold_Scripts'

HardcodedConfigurations.loop_picklist_file_path = \
    r'C:\bWa\OXi\etl\collect\blob\blob-temp-anusha_folder-sigraph_crosswalks_db_2023_04_20\sigraph_crosswalks\Loop_List_Cross_walk.xlsx'

# [0]=root_folder_path, [1]=storage type, [2]=parquet folder/xlsx file name, [3]=table name, [4]=registry type, [5]=id column names , [6]=xlsx sheet name, [7]=column names to set to default, [8]=Enum for filtered column names
FULL_USER_FILE_CONFIGURATION_LIST = \
    [
        # TODO: This are bronze requirements

        # TODO: This are silver requirements
        # 06 S CableCataloge Number Generation
        [USER_PARQUET_SILVER_SCRIPTS_ROOT_FOLDER_PATH, 'parquet', 'sigraph_silver', 'S_CableCoreCatalogue', 'input', None, ''],
        [USER_PARQUET_SILVER_SCRIPTS_ROOT_FOLDER_PATH, 'parquet', 'sigraph_silver', 'S_CableCatalogue', 'input', None, ''],
        [USER_PARQUET_SILVER_SCRIPTS_ROOT_FOLDER_PATH, 'parquet', 'sigraph_silver', 'S_CableCatalogueNumber_Master', 'original_output', None, ''],
        # 08 S Device Catalogue
        [USER_PARQUET_SILVER_SCRIPTS_ROOT_FOLDER_PATH, 'parquet', 'sigraph_silver', 'S_Itemfunction', 'input', None, ''],
        [USER_PARQUET_SILVER_SCRIPTS_ROOT_FOLDER_PATH, 'parquet', 'sigraph_silver', 'S_Item_Function_Model', 'input', None, ''],
        [USER_PARQUET_SILVER_SCRIPTS_ROOT_FOLDER_PATH, 'parquet', 'sigraph_silver', 'S_DeviceCatalogue', 'original_output', ['object_identifier'], '', ['description']],
        # 11 S Loop Index
        [USER_PARQUET_BRONZE_SCRIPTS_ROOT_FOLDER_PATH, 'parquet', 'sigraph_bronze', 'Layer', 'input', ['object_identifier']],
        [USER_PARQUET_BRONZE_SCRIPTS_ROOT_FOLDER_PATH, 'parquet', 'sigraph_bronze', 'Loop', 'input', ['loop_object_identifier']],
        [r'C:\bWa\OXi\etl\collect\blob\blob-temp-anusha_folder-sigraph_crosswalks_db_2023_04_20', 'parquet', 'sigraph_crosswalks', 'PBS_Table', 'input', None],
        [USER_PARQUET_SILVER_SCRIPTS_ROOT_FOLDER_PATH, 'parquet', 'sigraph_silver', 'S_Loop_Index', 'original_output', ['object_identifier']],
        # 17 S Terminations
        [USER_PARQUET_SILVER_SCRIPTS_ROOT_FOLDER_PATH, 'parquet', 'sigraph_silver', 'S_Connection', 'input', None, ''],
        [USER_PARQUET_SILVER_SCRIPTS_ROOT_FOLDER_PATH, 'parquet', 'sigraph_silver', 'S_Terminals', 'input', None, ''],
        [USER_PARQUET_SILVER_SCRIPTS_ROOT_FOLDER_PATH, 'parquet', 'sigraph_silver', 'S_ItemFunction', 'input', None, ''],
        [USER_PARQUET_SILVER_SCRIPTS_ROOT_FOLDER_PATH, 'parquet', 'sigraph_silver', 'S_Terminations', 'original_output', None, ''],
        # 18 S Internal Wiring
        [USER_PARQUET_SILVER_SCRIPTS_ROOT_FOLDER_PATH, 'parquet', 'sigraph_silver', 'S_Connection', 'input', None, ''],
        [USER_PARQUET_SILVER_SCRIPTS_ROOT_FOLDER_PATH, 'parquet', 'sigraph_silver', 'S_Terminals', 'input', None, ''],
        [USER_PARQUET_SILVER_SCRIPTS_ROOT_FOLDER_PATH, 'parquet', 'sigraph_silver', 'S_Internal_Wiring', 'original_output', ['object_identifier'], ''],

        # TODO: This are gold requirements
        # 06 S CableCataloge Number Generation
        [USER_LOADER_GOLD_SCRIPTS_ROOT_FOLDER_PATH, 'xlsx', '3_Cable_Catalogue.xlsx', 'Cable_Catalogue', 'original_output', None, 'export (20)'],
        [USER_LOADER_GOLD_SCRIPTS_ROOT_FOLDER_PATH, 'xlsx', '3_Cable_Core_Catalogue.xlsx', 'Cable_Core_Catalogue', 'original_output', None, 'export (21)'],
        # 08 S Device Catalogue
        [USER_LOADER_GOLD_SCRIPTS_ROOT_FOLDER_PATH, 'xlsx', '4_Device_Catalogue.xlsx', 'Device_Catalogue', 'original_output', None, 'export (22)'],
        # 11 S Loop Index
        [USER_LOADER_GOLD_SCRIPTS_ROOT_FOLDER_PATH, 'xlsx', '7_Loop_Index.xlsx', 'Loop_Index', 'original_output', None, 'export (25)'],
        # 17 S Terminations
        [USER_LOADER_GOLD_SCRIPTS_ROOT_FOLDER_PATH, 'xlsx', '13_Terminations.xlsx', 'Terminations', 'original_output', None, 'export (31)'],
        # 18 S Internal Wiring
        [USER_LOADER_GOLD_SCRIPTS_ROOT_FOLDER_PATH, 'xlsx', '14_Interal_Wiring.xlsx', 'Interal_Wiring', 'original_output', None, 'export (19)'],

    ]

FULL_USER_PROCESS_CONFIGURATION_LIST = \
    [
        # 06 S CableCataloge Number Generation
        run_silver_06_s_cablecatalogue_number_generation_sql_01_00,
        run_gold_c03_cable_catalogue_sql_01_00_using_new_outputs,
        run_gold_c03_cable_core_catalogue_sql_01_00_using_new_outputs,
        # 08 S Device Catalogue
        run_silver_08_s_device_catalogue_sql_01_00,
        run_gold_c04_device_catalogue_sql_01_00_using_new_outputs,
        # 11 S Loop Index
        run_silver_c11_s_loop_index_sql_01_00,
        run_gold_c07_loop_index_sql_01_00_v0_02,
        # 17 S Terminations
        run_silver_17_s_terminations_sql_01_00,
        run_gold_c13_terminations_sql_01_00_using_new_outputs,
        # 18 S Internal Wiring
        run_silver_18_s_internal_wiring_sql_01_00,
        run_gold_c14_internal_wiring_sql_01_00_using_new_outputs,

    ]

# GlobalFlags.RUN_PROCESS_FILES = \
#     False

GlobalFlags.RUN_BIEIZE = \
    True

GlobalFlags.RUN_BIEIZE_SANITY_CHECK = \
    False

GlobalFlags.RUN_BIEIZE_COMPARISON = \
    True

USER_OUTPUT_FOLDER_PREFIX = \
    's_device_catalogue_STN'
