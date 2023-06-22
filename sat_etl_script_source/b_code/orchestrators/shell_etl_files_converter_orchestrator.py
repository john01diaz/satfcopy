from nf_common_source.code.services.file_system_service.objects.folders import Folders
from nf_common_source.code.services.reporting_service.reporters.log_file import LogFiles
from nf_common_source.code.services.reporting_service.wrappers.run_and_log_function_wrapper import run_and_log_function
from sat_etl_script_source.b_code.exports.exported_csv_to_ms_access_database_loader import \
    load_exported_csv_to_ms_access_database
from sat_etl_script_source.b_code.orchestrators.shell_etl_files_converter_orchestrator_stage_01 import \
    orchestrate_shell_etl_files_converter_stage_01
from sat_etl_script_source.b_code.orchestrators.shell_etl_files_converter_orchestrator_stage_02 import \
    orchestrate_shell_etl_files_converter_stage_02
from sat_etl_script_source.b_code.orchestrators.shell_etl_files_converter_orchestrator_stage_02b import \
    orchestrate_shell_etl_files_converter_stage_02b
from sat_etl_script_source.b_code.orchestrators.shell_etl_files_converter_orchestrator_stage_04_py import \
    orchestrate_shell_etl_files_converter_stage_04_py
from sat_etl_script_source.b_code.orchestrators.shell_etl_files_converter_orchestrator_stage_04_sql import \
    orchestrate_shell_etl_files_converter_stage_04_sql
from sat_etl_script_source.b_code.orchestrators.shell_etl_files_converter_orchestrator_stage_05_sql import \
    orchestrate_shell_etl_files_converter_stage_05_sql
from sat_etl_script_source.b_code.orchestrators.shell_etl_files_converter_orchestrator_stage_06 import \
    orchestrate_shell_etl_files_converter_stage_06


@run_and_log_function
def orchestrate_shell_etl_files_converter(
        input_root_folder: Folders) \
        -> None:
    output_root_folder = \
        Folders(
            absolute_path_string=LogFiles.folder_path)

    # Note: Stage 1: split files by type (e.g. *.py, *.sql) - store output in a new folder
    orchestrate_shell_etl_files_converter_stage_01(
        input_root_folder=input_root_folder,
        stage_name='stage_01')

    # TODO: Stage 2: de-magic (remove the MAGIC strings) all files (SQL and .py files) - WIP
    orchestrate_shell_etl_files_converter_stage_02(
        input_root_folder=input_root_folder,
        previous_stage_name='stage_01',
        stage_name='stage_02')

    # TODO: Stage 2b: de-magic (remove the MAGIC strings) all files (SQL and .py files) and remove SQL comments - WIP
    orchestrate_shell_etl_files_converter_stage_02b(
        input_root_folder=input_root_folder,
        previous_stage_name='stage_01',
        stage_name='stage_02b')

    # # TODO: Stage 3: NOT TO BE DONE NOW
    # orchestrate_shell_etl_files_converter_stage_03_py(
    #     input_root_folder=input_root_folder,
    #     previous_stage_name='stage_02',
    #     stage_name='stage_03')

    # TODO: Stage 4: split all files by the COMMAND line
    orchestrate_shell_etl_files_converter_stage_04_py(
        input_root_folder=input_root_folder,
        previous_stage_name='stage_02',
        stage_name='stage_04')

    orchestrate_shell_etl_files_converter_stage_04_sql(
        input_root_folder=input_root_folder,
        previous_stage_name='stage_02',
        stage_name='stage_04')

    # TODO: Stage 5: split the SQL files by have/have not nominated text - currently stage 4
    orchestrate_shell_etl_files_converter_stage_05_sql(
        input_root_folder=input_root_folder,
        previous_stage_name='stage_04',
        stage_name='stage_05')

    # TODO: Stage 6: clean the SQL files - currently stage 5
    orchestrate_shell_etl_files_converter_stage_06(
        input_root_folder=input_root_folder,
        previous_stage_name='stage_05',
        stage_name='stage_06')

    load_exported_csv_to_ms_access_database(
        output_root_folder=output_root_folder)
