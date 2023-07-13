from nf_common_source.code.services.file_system_service.objects.folders import Folders
from nf_common_source.code.services.input_output_service.delimited_text.dataframe_dictionary_to_csv_files_writer import \
    write_dataframe_dictionary_to_csv_files
from sat_parquet_source.parquet_schema_analyzer.nf_common_to_move.code.services.table_as_dictionary_service.mapper_objects_to_table_converter import \
    convert_mapper_objects_to_table


def report_parquet_files_metrics(
        mapper_objects: set,
        table_name: str,
        output_folder: Folders) \
        -> dict:
    mapper_objects_as_tables_dictionary = \
        dict()

    mapper_objects_as_table = \
        convert_mapper_objects_to_table(
            mapper_objects=mapper_objects)

    mapper_objects_as_tables_dictionary[table_name] = \
        mapper_objects_as_table

    write_dataframe_dictionary_to_csv_files(
        folder_name=output_folder.absolute_path_string,
        dataframes_dictionary=mapper_objects_as_tables_dictionary)

    return \
        mapper_objects_as_tables_dictionary
