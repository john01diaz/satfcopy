import os.path
import pandas
from nf_common_source.code.services.file_system_service.objects.files import Files
from nf_common_source.code.services.identification_services.b_identity_service.b_identity_creators.b_identity_sum_from_strings_creator import \
    create_b_identity_sum_from_strings
from sat_parquet_source.parquet_schema_analyzer.b_code.b_identity_ecosystem.converters.int_to_base32_converter import convert_int_to_base32
from sat_parquet_source.parquet_schema_analyzer.b_code.common.objects.parquet_folders import ParquetFolders


class ParquetFiles:
    def __init__(
            self,
            input_parquet_file: Files,
            number_of_rows: int,
            parquet_file_content_as_table: pandas.DataFrame,
            parquet_folder_object_instance: ParquetFolders):
        self.bie_ids = \
            convert_int_to_base32(
                int_value=create_b_identity_sum_from_strings(
                    strings=[
                        str(parquet_folder_object_instance.bie_ids),
                        input_parquet_file.absolute_path_string
                    ]))

        self.number_of_columns = \
            parquet_file_content_as_table.shape[1]

        self.number_of_rows = \
            number_of_rows

        self.number_of_rows_filtered = \
            parquet_file_content_as_table.shape[0]

        self.absolute_path_strings = \
            input_parquet_file.absolute_path_string

        self.base_names = \
            input_parquet_file.base_name

        self.relative_paths = \
            os.path.relpath(
                str(input_parquet_file.absolute_path_string),
                str(parquet_folder_object_instance.parent_parquet_stage_folder_paths))

        self.owning_parquet_folder_bie_ids = \
            parquet_folder_object_instance.bie_ids
