# [0]=root_folder_path, [1]=storage type, [2]=parquet folder/xlsx file name, [3]=table name, [4]=registry type,
# [5]=id column names , [6]=xlsx sheet name, [7]=column names to set to default, [8]=Enum for filtered column names
from sat_workflow_source.b_code.etl_schemas.silver_stage.s_internal_wiring import S_Internal_Wiring

file_configuration_list = \
    [
        ##### BRONZE #####
        # [input_root_folder_parquet, 'parquet', 'sigraph_bronze', 'CS_Layer_Loop_Loop_elements', 'input', None, ''],
        ##### SILVER #####
        # [input_root_folder_parquet, 'parquet', 'sigraph_silver', 'S_CableCatalogue', 'input', None, ''],
        # [input_root_folder_parquet, 'parquet', 'sigraph_silver', 'S_CableCoreCatalogue', 'input', None, ''],
        # [input_root_folder_parquet, 'parquet', 'sigraph_silver', 'S_CableCatalogueNumber_Master', 'input', None, ''],
        # [input_root_folder_parquet, 'parquet', 'sigraph_silver', 'S_DeviceCatalogue', 'input', None, ''],
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
        
        ###### SILVER COLUMN EXCLUSION ######
        # [parquet_silver_root_folder_path, 'parquet', 'sigraph_silver', 'S_Internal_Wiring', 'original_output', None,
        #  None, [S_Internal_Wiring.TO_LEFT_RIGHT.value, S_Internal_Wiring.FROM_LEFT_RIGHT.value,
        #         S_Internal_Wiring.TO_WIRE_LINK.value, S_Internal_Wiring.TO_COMPARTMENT.value,
        #         S_Internal_Wiring.FROM_COMPARTMENT.value, S_Internal_Wiring.CLASS.value]],
        #
        # [parquet_silver_root_folder_path, 'parquet', 'sigraph_silver', 'S_CableCatalogueNumber_Master',
        #  'original_output', None, None, [S_CableCatalogueNumber_Master.CATALOGUENO.value]],
        # [parquet_silver_root_folder_path, 'parquet', 'sigraph_silver', 'S_Terminations', 'original_output', None,
        #  None, [S_Terminations.DATABASE_NAME.value, S_Terminations.OBJECT_IDENTIFIER.value,
        #         S_Terminations.PARENT_EQUIPMENT_NO.value]],
        
        ##### GOLD #####
        # [input_root_folder_gold_scripts, 'xlsx', '1_Project_Voltages.xlsx', 'Project_Voltages', 'original_output',
        # None, 'export (18)'],
        # [input_root_folder_gold_scripts, 'xlsx', '2_Instrument_Type_Catalogue.xlsx', 'Instrument_Type_Catalogue',
        # 'original_output', None, 'export (19)'],
        # [input_root_folder_gold_scripts, 'xlsx', '3_Cable_Catalogue.xlsx', 'Cable_Catalogue', 'original_output',
        # None, 'export (20)'],
        # [input_root_folder_gold_scripts, 'xlsx', '3_Cable_Core_Catalogue.xlsx', 'Cable_Core_Catalogue',
        # 'original_output', None, 'export (21)'],
        # [input_root_folder_gold_scripts, 'xlsx', '4_Device_Catalogue.xlsx', 'Device_Catalogue', 'original_output',
        # None, 'export (22)'],
        # [input_root_folder_gold_scripts, 'xlsx', '5_Field_Device_Catalogue.xlsx', 'Field_Device_Catalogue',
        # 'original_output', None, 'export (23)'],
        # [input_root_folder_gold_scripts, 'xlsx', '6_IO_Catalogue.xlsx', 'IO_Catalogue', 'original_output', None,
        # 'export (24)'],
        # [input_root_folder_gold_scripts, 'xlsx', '7_Loop_Index.xlsx', 'Loop_Index', 'original_output', None,
        # 'export (25)'],
        # [input_root_folder_gold_scripts, 'xlsx', '8_Instrument_Index.xlsx', 'Instrument_Index', 'original_output',
        # None, 'export (26)'],
        # [input_root_folder_gold_scripts, 'xlsx', '9_Major_Equipments.xlsx', 'Major_Equipments', 'original_output',
        # None, 'export (21)'],
        # [input_root_folder_gold_scripts, 'xlsx', '10_Component.xlsx', 'Component', 'original_output', None,
        # 'export (27)'],
        # [input_root_folder_gold_scripts, 'xlsx', '11_Cable_Schedule.xlsx', 'Cable_Schedule', 'original_output',
        # None, 'export (28)'],
        # [input_root_folder_gold_scripts, 'xlsx', '12_Terminals.xlsx', 'Terminals', 'original_output', None,
        # 'export (29)'],
        # [input_root_folder_gold_scripts, 'xlsx', '13_Terminations.xlsx', 'Terminations', 'original_output', None,
        # 'export (31)'],
        # [input_root_folder_gold_scripts, 'xlsx', '14_Interal_Wiring.xlsx', 'Interal_Wiring', 'original_output',
        # None, 'export (19)'],
        # [input_root_folder_gold_scripts, 'xlsx', '15_IO_Allocations.xlsx', 'IO_Allocations', 'original_output',
        # None, 'export (20)']
        ]

process_configuration_list = \
    [
        ##### SILVER #####
        # run_silver_18_s_internal_wiring_sql_01_00
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
