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
    input_root_folder_parquet = \
        r'C:\bWa\OXi\etl\collect\blob\blob-temp-anusha_folder-sigraph_silver_2023_04_19'

    input_root_folder_gold_scripts = \
        r'C:\bWa\OXi\etl\collect\loader_files\Gold_Scripts'
    
    output_root_folder = \
        Folders(
            absolute_path_string=r'C:\bWa\OXi\etl\main_wrapper_outputs')

    file_configuration_list = \
        [
            # [input_root_folder_parquet, 'parquet', 'sigraph_bronze', 'CS_Layer_Loop_Loop_elements', 'input', None, ''],
            [input_root_folder_parquet, 'parquet', 'sigraph_silver', 'S_CableCatalogue', 'input', None, ''],
            [input_root_folder_parquet, 'parquet', 'sigraph_silver', 'S_CableCoreCatalogue', 'input', None, ''],
            [input_root_folder_parquet, 'parquet', 'sigraph_silver', 'S_CableCatalogueNumber_Master', 'input', None, ''],
            [input_root_folder_parquet, 'parquet', 'sigraph_silver', 'S_DeviceCatalogue', 'input', None, ''],
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
        ]

    process_configuration_list = \
        [
            run_gold_c04_device_catalogue_sql_01_00,  # TODO: Fix the need for change of order (scientific notation bie_ids for the two first cable catalog bie_ids)
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
        output_folder_prefix='silver_tranche_1_etl_processes_wrapper',
        output_root_folder=output_root_folder,
        etl_processes_wrapper_configuration=etl_processes_wrapper_configuration)
