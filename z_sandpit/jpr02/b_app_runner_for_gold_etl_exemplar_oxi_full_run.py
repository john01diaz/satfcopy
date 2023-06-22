from nf_common_source.code.services.b_app_runner_service.b_app_runner import run_b_app
from nf_common_source.code.services.file_system_service.objects.folders import Folders
from nf_common_source.code.services.log_environment_utility_service.common_knowledge.environment_log_level_types import \
    EnvironmentLogLevelTypes
from sat_workflow_source.b_code.etl_processes.gold_layer.runners.c01_Project_Voltages_runner import \
    run_c01_project_voltages
from sat_workflow_source.b_code.etl_processes.gold_layer.runners.c02_Intrument_Type_Catalogue_runner import \
    run_c02_instrument_type_catalogue
from sat_workflow_source.b_code.etl_processes.gold_layer.runners.c03_Cable_Catalogue_runner import \
    run_c03_cable_catalogue
from sat_workflow_source.b_code.etl_processes.gold_layer.runners.c03_Cable_Core_Catalogue_runner import \
    run_c03_cable_core_catalogue
from sat_workflow_source.b_code.etl_processes.gold_layer.runners.c04_Device_Catalogue_runner import \
    run_c04_device_catalogue
from sat_workflow_source.b_code.etl_processes.gold_layer.runners.c05_Field_Device_Catalogue_runner import \
    run_c05_field_device_catalogue
from sat_workflow_source.b_code.etl_processes.gold_layer.runners.c06_IO_Catalogue_runner import run_c06_io_catalogue
from sat_workflow_source.b_code.etl_processes.gold_layer.runners.c07_Loop_Index_runner import run_c07_loop_index
from sat_workflow_source.b_code.etl_processes.gold_layer.runners.c08_Instrument_Index_runner import \
    run_c08_instrument_index
from sat_workflow_source.b_code.etl_processes.gold_layer.runners.c09_Major_Equipment_runner import \
    run_c09_major_equipment
from sat_workflow_source.b_code.etl_processes.gold_layer.runners.c10_Component_runner import run_c10_component
from sat_workflow_source.b_code.etl_processes.gold_layer.runners.c11_Cable_Schedule_runner import run_c11_cable_schedule
from sat_workflow_source.b_code.etl_processes.gold_layer.runners.c12_Terminals_runner import run_c12_terminals
from sat_workflow_source.b_code.etl_processes.gold_layer.runners.c13_Terminations_runner import run_c13_terminations
from sat_workflow_source.b_code.etl_processes.gold_layer.runners.c14_Internal_Wiring_runner import \
    run_c14_internal_wiring
from sat_workflow_source.b_code.etl_processes.gold_layer.runners.c15_IO_Allocations_runner import run_c15_io_allocations
from sat_workflow_source.b_code.etl_processes_wrapper.objects.etl_processes_wrapper_configurations import \
    EtlProcessesWrapperConfigurations
from sat_workflow_source.b_code.etl_processes_wrapper.orchestrators.etl_processes_wrapper_orchestrator import \
    orchestrate_etl_processes_wrapper

if __name__ == '__main__':
    input_root_folder_parquet = \
        r'D:\bWa\JPr\etl\collect\blob\blob-temp-anusha_folder-sigraph_silver_2023_04_19'
    
    input_root_folder_gold_scripts = \
        r'D:\bWa\JPr\etl\collect\loader_files\Gold_Scripts'
    
    output_root_folder = \
        Folders(
            absolute_path_string=r'D:\bWa\JPr\etl\main_wrapper_outputs')
    
    file_configuration_list = \
        [
            # [input_root_folder_parquet, 'parquet', 'sigraph_bronze', 'CS_Layer_Loop_Loop_elements', 'input', None,
            # ''],
            [input_root_folder_parquet, 'parquet', 'sigraph_silver', 'S_CableCatalogue', 'input', None, ''],
            [input_root_folder_parquet, 'parquet', 'sigraph_silver', 'S_CableCoreCatalogue', 'input', None, ''],
            [input_root_folder_parquet, 'parquet', 'sigraph_silver', 'S_CableCatalogueNumber_Master', 'input', None,
             ''],
            [input_root_folder_parquet, 'parquet', 'sigraph_silver', 'S_DeviceCatalogue', 'input', None, ''],
            # [input_root_folder_parquet, 'parquet', 'sigraph_silver', 'S_Field_Device_Catalogue', 'input', None, ''],
            # [input_root_folder_parquet, 'parquet', 'sigraph_silver', 'S_IO_Catalogue', 'input', None, ''],
            # [input_root_folder_parquet, 'parquet', 'sigraph_silver', 'S_Loop_Index', 'input', None, ''],
            # [input_root_folder_parquet, 'parquet', 'sigraph_silver', 'S_Instrument_Index', 'input', None, ''],
            # [input_root_folder_parquet, 'parquet', 'sigraph_silver', 'S_Major_Equipments', 'input', None, ''],
            # [input_root_folder_parquet, 'parquet', 'sigraph_silver', 'S_Component', 'input', None, ''],
            # [input_root_folder_parquet, 'parquet', 'sigraph_silver', 'S_CableSchedule', 'input', None, ''],
            # # [input_root_folder_parquet, 'parquet', 'sigraph_silver', 'S_Terminals', 'input', None, ''],
            # # [input_root_folder_parquet, 'parquet', 'sigraph_silver', 'S_Terminations', 'input', None, ''],
            # # [input_root_folder_parquet, 'parquet', 'sigraph_silver', 'S_Internal_Wiring', 'input', None, ''],
            # [input_root_folder_parquet, 'parquet', 'sigraph_silver', 'S_IO_Allocations', 'input', None, ''],
            
            # [input_root_folder_gold_scripts, 'xlsx', '1_Project_Voltages.xlsx', 'Project_Voltages',
            # 'original_output', None, 'export (18)'],
            # [input_root_folder_gold_scripts, 'xlsx', '2_Instrument_Type_Catalogue.xlsx',
            # 'Instrument_Type_Catalogue', 'original_output', None, 'export (19)'],
            [input_root_folder_gold_scripts, 'xlsx', '3_Cable_Catalogue.xlsx', 'Cable_Catalogue', 'original_output',
             None, 'export (20)'],
            [input_root_folder_gold_scripts, 'xlsx', '3_Cable_Core_Catalogue.xlsx', 'Cable_Core_Catalogue',
             'original_output', None, 'export (21)'],
            [input_root_folder_gold_scripts, 'xlsx', '4_Device_Catalogue.xlsx', 'Device_Catalogue', 'original_output',
             None, 'export (22)'],
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
            # # [input_root_folder_gold_scripts, 'xlsx', '14_Interal_Wiring.xlsx', 'Interal_Wiring',
            # 'original_output', None, 'export (19)'],
            # [input_root_folder_gold_scripts, 'xlsx', '15_IO_Allocations.xlsx', 'IO_Allocations', 'original_output',
            # None, 'export (20)']
            
            ]
    
    process_configuration_list = \
        [
            # run_c01_project_voltages,
            # run_c02_instrument_type_catalogue,
            run_c03_cable_catalogue,
            run_c03_cable_core_catalogue,
            run_c04_device_catalogue,
            # run_c05_field_device_catalogue,
            # run_c06_io_catalogue,
            # run_c07_loop_index,
            # run_c08_instrument_index,
            # run_c09_major_equipment,
            # run_c10_component,
            # run_c11_cable_schedule,
            # # run_c12_terminals,
            # # run_c13_terminations,
            # # run_c14_internal_wiring,
            # run_c15_io_allocations
            ]
    
    etl_processes_wrapper_configuration = \
        EtlProcessesWrapperConfigurations(
            file_configuration_list=file_configuration_list,
            process_configuration_list=process_configuration_list,
            run_new_vs_original_comparison=False)
    
    run_b_app(
        app_startup_method=orchestrate_etl_processes_wrapper,
        environment_log_level_type=EnvironmentLogLevelTypes.FILTERED,
        output_folder_prefix='gold_etl_processes_wrapper_full',
        output_root_folder=output_root_folder,
        etl_processes_wrapper_configuration=etl_processes_wrapper_configuration)
