from nf_common_source.code.services.file_system_service.objects.folders import Folders
from sat_parquet_source.b_code.common_knowledge.pyspark_output_parser_constants import PYSPARK_OUTPUTS_PARSER_DATABASE_FILE_NAME
from sat_parquet_source.b_code.reporters.exporters.csv_files_to_ms_access_exporter import export_csv_files_to_ms_access
from sat_parquet_source.b_code.reporters.exporters.dictionary_of_dataframes_to_sqlite_exporter import export_dictionary_of_dataframes_to_sqlite
from sat_parquet_source.b_code.reporters.parquet_files_metrics_reporter import report_parquet_files_metrics
from sat_parquet_source.b_code.reporters.exporters.parquet_files_to_csv_exporter import export_parquet_files_to_csv


def orchestrate_parquet_folder_report(
        working_output_folder: Folders,
        output_folder: Folders,
        output_file_suffix: str,
        datetime_stamp: str,
        parquet_folders_as_tables: set,
        parquet_files_as_tables: set,
        parquet_file_columns_as_tables: set,
        pyspark_dataframe_to_pandas_dataframe_dictionary: dict,
        parquet_files_dictionary: dict,
        export_to_access: bool,
        export_to_sqlite: bool,
        export_csvs_to_sqlite: bool,
        export_parquet_file_to_csv: bool):
    parquet_folders_as_tables_dictionary = \
        report_parquet_files_metrics(
            mapper_objects=parquet_folders_as_tables,
            table_name='parquet_folders',
            output_folder=output_folder)

    parquet_files_as_tables_dictionary = \
        report_parquet_files_metrics(
            mapper_objects=parquet_files_as_tables,
            table_name='parquet_files',
            output_folder=output_folder)

    parquet_file_columns_as_tables_dictionary = \
        report_parquet_files_metrics(
            mapper_objects=parquet_file_columns_as_tables,
            table_name='parquet_file_columns',
            output_folder=output_folder)

    if export_parquet_file_to_csv:
        export_parquet_files_to_csv(
            working_output_folder=working_output_folder,
            pyspark_dataframe_to_pandas_dataframe_dictionary=pyspark_dataframe_to_pandas_dataframe_dictionary,
            parquet_files_dictionary=parquet_files_dictionary)

    if export_to_access:
        export_csv_files_to_ms_access(
            output_file_suffix=output_file_suffix,
            working_output_folder=output_folder,
            datetime_stamp=datetime_stamp)

    if export_to_sqlite:
        sqlite_database_base_name = \
            PYSPARK_OUTPUTS_PARSER_DATABASE_FILE_NAME + '_' + output_file_suffix + '_' + datetime_stamp

        sqlite_database_file = \
            export_dictionary_of_dataframes_to_sqlite(
                dictionary_of_tables_to_export=pyspark_dataframe_to_pandas_dataframe_dictionary,
                sqlite_database_base_name=sqlite_database_base_name,
                export_csvs_to_sqlite=export_csvs_to_sqlite,
                sqlite_database_folder=output_folder)

        export_dictionary_of_dataframes_to_sqlite(
            dictionary_of_tables_to_export=parquet_folders_as_tables_dictionary,
            sqlite_database_base_name=sqlite_database_base_name,
            sqlite_database_folder=output_folder,
            sqlite_database_file=sqlite_database_file,
            database_exists=True)

        export_dictionary_of_dataframes_to_sqlite(
            dictionary_of_tables_to_export=parquet_files_as_tables_dictionary,
            sqlite_database_base_name=sqlite_database_base_name,
            sqlite_database_folder=output_folder,
            sqlite_database_file=sqlite_database_file,
            database_exists=True)

        export_dictionary_of_dataframes_to_sqlite(
            dictionary_of_tables_to_export=parquet_file_columns_as_tables_dictionary,
            sqlite_database_base_name=sqlite_database_base_name,
            sqlite_database_folder=output_folder,
            sqlite_database_file=sqlite_database_file,
            database_exists=True)
