from nf_common_source.code.services.file_system_service.objects.files import Files
from nf_common_source.code.services.file_system_service.objects.folders import Folders
from nf_common_source.code.services.input_output_service.sqlite.dataframe_to_sqlite_writer import \
    write_dataframe_to_sqlite
from nf_common_source.code.services.input_output_service.sqlite.sqlite_database_creator import create_sqlite_database


def export_dictionary_of_dataframes_to_sqlite(
        dictionary_of_tables_to_export: dict,
        sqlite_database_base_name: str,
        sqlite_database_folder: Folders,
        export_csvs_to_sqlite: bool = True,
        sqlite_database_file: Files = None,
        database_exists: bool = False) \
        -> Files:
    if not database_exists:
        sqlite_database_file = \
            create_sqlite_database(
                sqlite_database_folder=sqlite_database_folder,
                sqlite_database_base_name=sqlite_database_base_name)

    __export_dataframes_to_sqlite_database(
        dictionary_of_tables_to_export=dictionary_of_tables_to_export,
        sqlite_database_file=sqlite_database_file,
        export_csvs_to_sqlite=export_csvs_to_sqlite)

    return \
        sqlite_database_file


def __export_dataframes_to_sqlite_database(
        dictionary_of_tables_to_export: dict,
        sqlite_database_file: Files,
        export_csvs_to_sqlite: bool):
    if export_csvs_to_sqlite:
        for table_name, table \
                in dictionary_of_tables_to_export.items():
            write_dataframe_to_sqlite(
                dataframe=table,
                table_name=table_name,
                sqlite_database_file=sqlite_database_file)
