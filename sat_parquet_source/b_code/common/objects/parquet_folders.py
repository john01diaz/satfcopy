import os
from nf_common_source.code.services.file_system_service.objects.folders import Folders
from nf_common_source.code.services.identification_services.b_identity_service.b_identity_creators.b_identity_sum_from_strings_creator import \
    create_b_identity_sum_from_strings
from sat_parquet_source.b_code.b_identity_ecosystem.converters.int_to_base32_converter import convert_int_to_base32


class ParquetFolders:
    def __init__(
            self,
            database_folder: Folders):
        self.bie_ids = \
            convert_int_to_base32(
                int_value=create_b_identity_sum_from_strings(
                    strings=[database_folder.absolute_path_string]))

        self.parquet_folder_names = \
            database_folder.base_name

        self.parquet_folder_paths = \
            database_folder.absolute_path_string

        self.parent_parquet_stage_folder_paths = \
            database_folder.parent_absolute_path_string

        self.stage = \
            database_folder.parent_absolute_path_string.split(os.sep)[-1]

        self.parent_parquet_stage_folder_relative_paths = \
            os.sep.join(
                database_folder.parent_absolute_path_string.split(os.sep)[5:])

        self.parent_folder_bie_ids = \
            convert_int_to_base32(
                int_value=create_b_identity_sum_from_strings(
                    strings=[self.parent_parquet_stage_folder_relative_paths]))
