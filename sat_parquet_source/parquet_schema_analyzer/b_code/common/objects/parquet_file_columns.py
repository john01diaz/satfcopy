from nf_common_source.code.services.identification_services.b_identity_service.b_identity_creators.b_identity_sum_from_strings_creator import \
    create_b_identity_sum_from_strings
from sat_parquet_source.parquet_schema_analyzer.b_code.b_identity_ecosystem.converters.int_to_base32_converter import convert_int_to_base32
from sat_parquet_source.parquet_schema_analyzer.b_code.common.objects.parquet_files import ParquetFiles
from sat_parquet_source.parquet_schema_analyzer.b_code.common.objects.parquet_folders import ParquetFolders


class ParquetFileColumns:
    def __init__(
            self,
            column_name: str,
            datatypes: str,
            sql_column_dtype: str,
            parquet_file_object_instance: ParquetFiles,
            parquet_folder_object_instance: ParquetFolders):
        self.bie_ids = \
            convert_int_to_base32(
                int_value=create_b_identity_sum_from_strings(
                    strings=[
                        str(parquet_folder_object_instance.bie_ids),
                        column_name
                    ]))

        self.column_names = \
            column_name

        self.parquet_file_datatypes = \
            datatypes

        self.sql_datatypes = \
            sql_column_dtype.upper() if sql_column_dtype else sql_column_dtype

        self.owning_parquet_folder_bie_ids = \
            parquet_folder_object_instance.bie_ids

        self.owning_parquet_folder_names = \
            parquet_folder_object_instance.parquet_folder_names
