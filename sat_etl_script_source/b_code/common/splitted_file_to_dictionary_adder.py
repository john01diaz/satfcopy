import os

from nf_common_source.code.services.identification_services.b_identity_ecosystem.converters.int_to_base32_converter import \
    convert_int_to_base32
from nf_common_source.code.services.identification_services.b_identity_service.b_identity_creators.b_identity_sum_from_strings_creator import \
    create_b_identity_sum_from_strings


def add_splitted_file_to_dictionary(
        splitted_files_dictionary: dict,
        relative_path,
        filename,
        source_filename,
        source_type,
        line_count,
        type,
        is_empty,
        position_in_file,
        position_in_statements,
        is_magic: bool = None):
    row = \
        {
            'filenames': filename,
            'relative_paths': relative_path,
            'parent_folder_names': relative_path.split(os.sep)[-2],
            'source_filenames': source_filename,
            'source_types': source_type,
            'line_counts': line_count,
            'types': type,
            'is_magic': is_magic,
            'is_empty': is_empty,
            'position_in_file': position_in_file,
            'position_in_statements': position_in_statements,
        }

    row_values_as_strings = \
        [str(value) for value in list(row.values())]

    bie_ids = \
        convert_int_to_base32(
            int_value=create_b_identity_sum_from_strings(
                strings=row_values_as_strings))

    row['bie_ids'] = \
        bie_ids

    etl_original_file_bie_ids = \
        convert_int_to_base32(
            int_value=create_b_identity_sum_from_strings(
                strings=[relative_path, source_filename]))

    row['etl_original_file_bie_ids'] = \
        etl_original_file_bie_ids

    splitted_files_dictionary[len(splitted_files_dictionary)] = \
        row
