import os
from nf_common_source.code.services.reporting_service.reporters.log_with_datetime import log_message


def get_parquet_folder_path(
        file_configuration: list) \
        -> str:
    user_specific_absolute_path_to_relative_path = \
        file_configuration[0]

    stage_folder_name = \
        file_configuration[2]

    file_name_folder = \
        file_configuration[3]

    log_message(
        message='Running on: ' + str(stage_folder_name) + ' - ' + str(file_name_folder))

    parquet_folder_path = \
        os.path.join(
            user_specific_absolute_path_to_relative_path,
            stage_folder_name,
            file_name_folder)

    return \
        parquet_folder_path
