import glob
from nf_common_source.code.services.datetime_service.time_helpers.time_getter import now_time_as_string_for_files
from nf_common_source.code.services.file_system_service.objects.folders import Folders
from nf_common_source.code.services.reporting_service.wrappers.run_and_log_function_wrapper import run_and_log_function
from sat_parquet_source.b_code.common.output_folder_getter import get_output_folder
from sat_parquet_source.b_code.helpers.working_output_folder_getter import get_working_output_folder
from sat_parquet_source.b_code.orchestrators.parquet_file_reader_orchestrator import orchestrate_parquet_file_reader
from sat_parquet_source.b_code.orchestrators.parquet_folder_report_orchestrator import orchestrate_parquet_folder_report
from sat_parquet_source.b_code.reporters.populators.parquet_objects_as_tables_populator import populate_parquet_objects_as_tables


@run_and_log_function
def orchestrate_parquet_folder_consumer(
        input_root_folder: Folders,
        session_name_string: str,
        output_file_suffix: str,
        data_chunk_size: int = None,
        export_to_access: bool = False,
        export_to_sqlite: bool = True,
        export_csvs_to_sqlite: bool = True,
        export_parquet_file_to_csv: bool = False) \
        -> None:
    datetime_stamp = \
        now_time_as_string_for_files()

    parquet_files_container_dictionary = \
        dict()

    pyspark_dataframe_to_pandas_dataframe_dictionary = \
        dict()

    parquet_folders_as_tables = \
        set()

    parquet_files_as_tables = \
        set()

    parquet_file_columns_as_tables = \
        set()

    output_folder = \
        get_output_folder()

    working_output_folder = \
        get_working_output_folder(
            output_folder=output_folder)

    __process_parquet_folder(
        parquet_folders_as_tables=parquet_folders_as_tables,
        parquet_files_as_tables=parquet_files_as_tables,
        parquet_file_columns_as_tables=parquet_file_columns_as_tables,
        input_root_folder=input_root_folder,
        data_chunk_size=data_chunk_size,
        working_output_folder=working_output_folder,
        parquet_files_container_dictionary=parquet_files_container_dictionary,
        session_name_string=session_name_string)

    try:
        orchestrate_parquet_folder_report(
            working_output_folder=working_output_folder,
            output_folder=output_folder,
            output_file_suffix=output_file_suffix,
            datetime_stamp=datetime_stamp,
            parquet_folders_as_tables=parquet_folders_as_tables,
            parquet_files_as_tables=parquet_files_as_tables,
            parquet_file_columns_as_tables=parquet_file_columns_as_tables,
            pyspark_dataframe_to_pandas_dataframe_dictionary=pyspark_dataframe_to_pandas_dataframe_dictionary,
            parquet_files_dictionary=parquet_files_container_dictionary,
            export_to_access=export_to_access,
            export_to_sqlite=export_to_sqlite,
            export_csvs_to_sqlite=export_csvs_to_sqlite,
            export_parquet_file_to_csv=export_parquet_file_to_csv)

    except Exception as error:
        print(
            'ERROR: ' + str(error) + ' - ' + input_root_folder.base_name)


def __process_parquet_folder(
        parquet_folders_as_tables: set,
        parquet_files_as_tables: set,
        parquet_file_columns_as_tables: set,
        input_root_folder: Folders,
        data_chunk_size: int,
        working_output_folder: Folders,
        parquet_files_container_dictionary: dict,
        session_name_string: str) \
        -> None:
    input_root_folder_children = \
        glob.glob(
            input_root_folder.absolute_path_string + "/*",
            recursive=True)

    for input_root_folder_child_path \
            in input_root_folder_children:
        orchestrate_parquet_file_reader(
            session_name_string=session_name_string,
            data_chunk_size=data_chunk_size,
            working_output_folder=working_output_folder,
            parquet_files_container_dictionary=parquet_files_container_dictionary,
            input_root_folder_child_path=input_root_folder_child_path)

        populate_parquet_objects_as_tables(
            parquet_files_container_dictionary=parquet_files_container_dictionary,
            parquet_folders_as_tables=parquet_folders_as_tables,
            parquet_files_as_tables=parquet_files_as_tables,
            parquet_file_columns_as_tables=parquet_file_columns_as_tables)
