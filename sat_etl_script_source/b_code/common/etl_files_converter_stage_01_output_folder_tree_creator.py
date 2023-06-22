import os
from nf_common_source.code.services.file_system_service.objects.folders import Folders
from nf_common_source.code.services.reporting_service.reporters.log_file import LogFiles
from sat_etl_script_source.b_code.common.helpers.new_folder_creator import create_new_folder


def create_etl_files_converter_stage_01_output_folder_tree(
        stage_name: str) \
        -> tuple:
    stage_01_folder_path = \
        os.path.join(
            LogFiles.folder_path,
            stage_name)

    create_new_folder(
        folder_path=stage_01_folder_path)

    sql_extension_folder_path = \
        os.path.join(
            LogFiles.folder_path,
            stage_name,
            'sql_ext')

    create_new_folder(
        folder_path=sql_extension_folder_path)

    sql_extension_folder = \
        Folders(
            absolute_path_string=sql_extension_folder_path)

    python_extension_folder_path = \
        os.path.join(
            LogFiles.folder_path,
            stage_name,
            'py_ext')

    create_new_folder(
        folder_path=python_extension_folder_path)

    python_extension_folder = \
        Folders(
            absolute_path_string=python_extension_folder_path)

    return \
        sql_extension_folder, \
        python_extension_folder
