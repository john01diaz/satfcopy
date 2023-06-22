from nf_common_source.code.services.b_app_runner_service.b_app_runner import run_b_app
from nf_common_source.code.services.file_system_service.objects.folders import Folders
from nf_common_source.code.services.log_environment_utility_service.common_knowledge.environment_log_level_types import \
    EnvironmentLogLevelTypes
from sat_workflow_source.b_code.etl_processes.etl_layers.python.runners import \
    run_gold_c03_cable_catalogue_sql_01_00
from z_old_code.etl_processes.etl_layers.runners_v2.gold import \
    run_gold_c03_cable_core_catalogue_sql_01_00
from z_old_code.etl_processes.etl_layers.runners_v2.gold import \
    run_gold_c04_device_catalogue_sql_01_00
from sat_workflow_source.b_code.etl_processes_wrapper.objects.etl_processes_wrapper_configurations import \
    EtlProcessesWrapperConfigurations
from sat_workflow_source.b_code.etl_processes_wrapper.orchestrators.etl_processes_wrapper_orchestrator import \
    orchestrate_etl_processes_wrapper


if __name__ == '__main__':
    output_root_folder = \
        Folders(
            absolute_path_string=r'C:\bWa\OXi\etl\main_wrapper_outputs')

    file_configuration_list = \
        [
            # [r'C:\bWa\AMi\etl\collect\blob\blob-temp-anusha_folder-sigraph_bronze_2023_04_19', 'sigraph_bronze', 'CS_Layer_Loop_Loop_elements', 'input', None, ''],
            [r'C:\bWa\OXi\etl\collect\blob\blob-temp-anusha_folder-sigraph_silver_2023_04_19', 'parquet', 'sigraph_silver', 'S_CableCatalogue', 'input', None, ''],
            [r'C:\bWa\OXi\etl\collect\blob\blob-temp-anusha_folder-sigraph_silver_2023_04_19', 'parquet', 'sigraph_silver', 'S_CableCoreCatalogue', 'input', None, ''],
            [r'C:\bWa\OXi\etl\collect\blob\blob-temp-anusha_folder-sigraph_silver_2023_04_19', 'parquet', 'sigraph_silver', 'S_CableCatalogueNumber_Master', 'input', None, ''],
            [r'C:\bWa\OXi\etl\collect\blob\blob-temp-anusha_folder-sigraph_silver_2023_04_19', 'parquet', 'sigraph_silver', 'S_DeviceCatalogue', 'input', None, ''],
            # [r'C:\bWa\AMi\etl\collect\blob\blob-temp-anusha_folder-sigraph_silver_2023_04_19', 'parquet', 'sigraph_silver', 'S_Field_Device_Catalogue', 'input', None, ''],
            # [r'C:\bWa\AMi\etl\collect\blob\blob-temp-anusha_folder-sigraph_silver_2023_04_19', 'parquet', 'sigraph_silver', 'S_IO_Catalogue', 'input', None, ''],
            # [r'C:\bWa\AMi\etl\collect\blob\blob-temp-anusha_folder-sigraph_silver_2023_04_19', 'parquet', 'sigraph_silver', 'S_Loop_Index', 'input', None, ''],
            # [r'C:\bWa\AMi\etl\collect\blob\blob-temp-anusha_folder-sigraph_silver_2023_04_19', 'parquet', 'sigraph_silver', 'S_Instrument_Index', 'input', None, ''],
            # [r'C:\bWa\AMi\etl\collect\blob\blob-temp-anusha_folder-sigraph_silver_2023_04_19', 'parquet', 'sigraph_silver', 'S_Major_Equipments', 'input', None, ''],
            # [r'C:\bWa\AMi\etl\collect\blob\blob-temp-anusha_folder-sigraph_silver_2023_04_19', 'parquet', 'sigraph_silver', 'S_Component', 'input', None, ''],
            # [r'C:\bWa\AMi\etl\collect\blob\blob-temp-anusha_folder-sigraph_silver_2023_04_19', 'parquet', 'sigraph_silver', 'S_CableSchedule', 'input', None, ''],
            # [r'C:\bWa\AMi\etl\collect\blob\blob-temp-anusha_folder-sigraph_silver_2023_04_19', 'parquet', 'sigraph_silver', 'S_Terminals', 'input', None, ''],
            # [r'C:\bWa\AMi\etl\collect\blob\blob-temp-anusha_folder-sigraph_silver_2023_04_19', 'parquet', 'sigraph_silver', 'S_Terminations', 'input', None, ''],
            # [r'C:\bWa\AMi\etl\collect\blob\blob-temp-anusha_folder-sigraph_silver_2023_04_19', 'parquet', 'sigraph_silver', 'S_Internal_Wiring', 'input', None, ''],
            # [r'C:\bWa\AMi\etl\collect\blob\blob-temp-anusha_folder-sigraph_silver_2023_04_19', 'parquet', 'sigraph_silver', 'S_IO_Allocations', 'input', None, ''],

            [r'C:\bWa\OXi\etl\collect\loader_files\Gold_Scripts', 'xlsx', '1_Project_Voltages.xlsx', 'Project_Voltages', 'original_output', None, 'export (18)'],
            [r'C:\bWa\OXi\etl\collect\loader_files\Gold_Scripts', 'xlsx', '2_Instrument_Type_Catalogue.xlsx', 'Instrument_Type_Catalogue', 'original_output', None, 'export (19)'],
            [r'C:\bWa\OXi\etl\collect\loader_files\Gold_Scripts', 'xlsx', '3_Cable_Catalogue.xlsx', 'Cable_Catalogue', 'original_output', None, 'export (20)'],
            [r'C:\bWa\OXi\etl\collect\loader_files\Gold_Scripts', 'xlsx', '3_Cable_Core_Catalogue.xlsx', 'Cable_Core_Catalogue', 'original_output', None, 'export (21)'],
            [r'C:\bWa\OXi\etl\collect\loader_files\Gold_Scripts', 'xlsx', '4_Device_Catalogue.xlsx', 'Device_Catalogue', 'original_output', None, 'export (22)'],
            # [r'C:\bWa\AMi\etl\collect\loader_files\Gold_Scripts', 'xlsx', '5_Field_Device_Catalogue.xlsx', 'Field_Device_Catalogue', 'original_output', None, 'export (23)'],
            # [r'C:\bWa\AMi\etl\collect\loader_files\Gold_Scripts', 'xlsx', '6_IO_Catalogue.xlsx', 'IO_Catalogue', 'original_output', None, 'export (24)'],
            # [r'C:\bWa\AMi\etl\collect\loader_files\Gold_Scripts', 'xlsx', '7_Loop_Index.xlsx', 'Loop_Index', 'original_output', None, 'export (25)'],
            # [r'C:\bWa\AMi\etl\collect\loader_files\Gold_Scripts', 'xlsx', '8_Instrument_Index.xlsx', 'Instrument_Index', 'original_output', None, 'export (26)'],
            # [r'C:\bWa\AMi\etl\collect\loader_files\Gold_Scripts', 'xlsx', '9_Major_Equipments.xlsx', 'Major_Equipments', 'original_output', None, 'export (21)'],
            # [r'C:\bWa\AMi\etl\collect\loader_files\Gold_Scripts', 'xlsx', '10_Component.xlsx', 'Component', 'original_output', None, 'export (27)'],
            # [r'C:\bWa\AMi\etl\collect\loader_files\Gold_Scripts', 'xlsx', '11_Cable_Schedule.xlsx', 'Cable_Schedule', 'original_output', None, 'export (28)'],
            # [r'C:\bWa\AMi\etl\collect\loader_files\Gold_Scripts', 'xlsx', '12_Terminals.xlsx', 'Terminals', 'original_output', None, 'export (29)'],
            # [r'C:\bWa\AMi\etl\collect\loader_files\Gold_Scripts', 'xlsx', '13_Terminations.xlsx', 'Terminations', 'original_output', None, 'export (31)'],
            # [r'C:\bWa\AMi\etl\collect\loader_files\Gold_Scripts', 'xlsx', '14_Interal_Wiring.xlsx', 'Interal_Wiring', 'original_output', None, 'export (19)'],
            # [r'C:\bWa\AMi\etl\collect\loader_files\Gold_Scripts', 'xlsx', '15_IO_Allocations.xlsx', 'IO_Allocations', 'original_output', None, 'export (20)']
        ]

    process_configuration_list = \
        [
            run_gold_c04_device_catalogue_sql_01_00,
            run_gold_c03_cable_core_catalogue_sql_01_00,
            run_gold_c03_cable_catalogue_sql_01_00
        ]

    etl_processes_wrapper_configuration = \
        EtlProcessesWrapperConfigurations(
            file_configuration_list=file_configuration_list,
            process_configuration_list=process_configuration_list)

    run_b_app(
        app_startup_method=orchestrate_etl_processes_wrapper,
        environment_log_level_type=EnvironmentLogLevelTypes.FILTERED,
        output_folder_prefix='gold_etl_processes_wrapper',
        output_root_folder=output_root_folder,
        etl_processes_wrapper_configuration=etl_processes_wrapper_configuration)
