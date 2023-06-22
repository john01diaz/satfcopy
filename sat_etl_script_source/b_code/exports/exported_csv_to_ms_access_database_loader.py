import os
import shutil
from pathlib import Path
from nf_common_source.code.services.datetime_service.time_helpers.time_getter import now_time_as_string_for_files
from nf_common_source.code.services.file_system_service.objects.files import Files
from nf_common_source.code.services.file_system_service.objects.folders import Folders
from nf_common_source.code.services.input_output_service.access.all_csv_files_from_folder_to_access_exporter import \
    export_all_csv_files_from_folder_to_access


def load_exported_csv_to_ms_access_database(
        output_root_folder: Folders) \
        -> None:
    new_database_file = \
        __create_new_database_file_from_template(
            output_root_folder=output_root_folder)

    export_all_csv_files_from_folder_to_access(
        csv_folder=output_root_folder,
        database_already_exists=True,
        database_full_path_if_already_exists=new_database_file.absolute_path_string)


def __create_new_database_file_from_template(
        output_root_folder: Folders) \
        -> Files:
    current_project_folder = \
        Folders(
            absolute_path_string=Path(__file__).parent.parent.parent.parent.__str__())

    access_database_template_file = \
        Files(
            absolute_path_string=os.path.join(
                current_project_folder.absolute_path_string,
                'sat_etl_script_source',
                'b_code',
                'resources',
                'templates',
                'etl_scripts_folders_files_empty_template.accdb'))

    new_database_file_name = \
        access_database_template_file.base_name.replace(
            'empty_template',
            now_time_as_string_for_files())

    new_database_file = \
        Files(absolute_path_string=os.path.join(
            output_root_folder.absolute_path_string,
            new_database_file_name))

    shutil.copyfile(
        access_database_template_file.absolute_path_string,
        os.path.join(
            output_root_folder.absolute_path_string,
            new_database_file.absolute_path_string))

    return \
        new_database_file
