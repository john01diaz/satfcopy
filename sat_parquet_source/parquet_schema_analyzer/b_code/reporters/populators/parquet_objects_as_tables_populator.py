import pandas
from nf_common_source.code.services.file_system_service.objects.files import Files
from nf_common_source.code.services.file_system_service.objects.folders import Folders
from sat_parquet_source.parquet_schema_analyzer.b_code.common.objects.parquet_file_columns import ParquetFileColumns
from sat_parquet_source.parquet_schema_analyzer.b_code.common.objects.parquet_files import ParquetFiles
from sat_parquet_source.parquet_schema_analyzer.b_code.common.objects.parquet_folders import ParquetFolders
from sat_parquet_source.parquet_schema_analyzer.b_code.helpers.sql_datatype_name_from_parquet_datatype_name_getter import \
    get_sql_datatype_name_from_parquet_datatype_name


def populate_parquet_objects_as_tables(
        parquet_files_container_dictionary: dict,
        parquet_folders_as_tables: set,
        parquet_files_as_tables: set,
        parquet_file_columns_as_tables: set) \
        -> None:
    for child_parquet_folder, parquet_files_as_table_dictionary \
            in parquet_files_container_dictionary.items():
        for child_parquet_file, parquet_file_content_dictionary \
                in parquet_files_as_table_dictionary.items():
            __add_parquet_objects_as_table(
                parquet_file_content_as_table=parquet_file_content_dictionary['data'],
                number_of_rows=parquet_file_content_dictionary['row_count'],
                dtypes=parquet_file_content_dictionary['dtypes'],
                parquet_folders_as_tables=parquet_folders_as_tables,
                parquet_file_columns_as_tables=parquet_file_columns_as_tables,
                parquet_files_as_tables=parquet_files_as_tables,
                child_parquet_folder=child_parquet_folder,
                child_parquet_file=child_parquet_file)


def __add_parquet_objects_as_table(
        parquet_file_content_as_table: pandas.DataFrame,
        number_of_rows: int,
        dtypes: list,
        parquet_folders_as_tables: set,
        parquet_file_columns_as_tables: set,
        parquet_files_as_tables: set,
        child_parquet_folder: Folders,
        child_parquet_file: Files) \
        -> None:
    parquet_folder_object_instance = \
        ParquetFolders(
            database_folder=child_parquet_folder)

    parquet_folders_as_tables.add(
        parquet_folder_object_instance)

    parquet_file_object_instance = \
        ParquetFiles(
            input_parquet_file=child_parquet_file,
            number_of_rows=number_of_rows,
            parquet_file_content_as_table=parquet_file_content_as_table,
            parquet_folder_object_instance=parquet_folder_object_instance)

    parquet_files_as_tables.add(
        parquet_file_object_instance)

    __add_parquet_files_columns_as_table(
        dtypes=dtypes,
        parquet_file_columns_as_tables=parquet_file_columns_as_tables,
        parquet_file_content_as_table=parquet_file_content_as_table,
        parquet_file_object_instance=parquet_file_object_instance,
        parquet_folder_object_instance=parquet_folder_object_instance)


def __add_parquet_files_columns_as_table(
        dtypes: list,
        parquet_file_columns_as_tables: set,
        parquet_file_content_as_table: pandas.DataFrame,
        parquet_file_object_instance: ParquetFiles,
        parquet_folder_object_instance: ParquetFolders) \
        -> None:
    for column_name \
            in list(parquet_file_content_as_table.columns):
        if column_name == 'parquet_file_relative_path':
            continue

        original_column_dtype = \
            __get_original_column_dtype(
                dtypes=dtypes,
                column_name=column_name)

        sql_column_dtype = \
            __get_sql_column_dtype(
                dtypes=dtypes,
                column_name=column_name)

        parquet_file_column = \
            ParquetFileColumns(
                column_name=column_name,
                datatypes=original_column_dtype,
                sql_column_dtype=sql_column_dtype,
                parquet_file_object_instance=parquet_file_object_instance,
                parquet_folder_object_instance=parquet_folder_object_instance)

        parquet_file_columns_as_tables.add(
            parquet_file_column)


def __get_sql_column_dtype(
        dtypes: list,
        column_name: str) \
        -> str:
    for dtype \
            in dtypes:
        if dtype.name == column_name:
            sql_data_type = \
                get_sql_datatype_name_from_parquet_datatype_name(
                    spark_data_type=dtype.dataType)

            return \
                sql_data_type


def __get_original_column_dtype(
        dtypes: list,
        column_name: str) -> str:
    for dtype \
            in dtypes:
        if dtype.name == column_name:
            return \
                dtype.dataType.typeName()
