import os.path
import os
from nf_common_source.code.services.file_system_service.new_folder_creator import create_new_folder
from nf_common_source.code.services.file_system_service.objects.folders import Folders


def get_working_output_folder(
        output_folder: Folders) \
        -> Folders:
    working_output_folder = \
        Folders(
            absolute_path_string=os.path.join(
                output_folder.absolute_path_string,
                'working'))

    if not os.path.exists(working_output_folder.parent_absolute_path_string):
        create_new_folder(
            parent_folder_path=working_output_folder.parent_absolute_path_string,
            new_folder_name=working_output_folder.base_name)

    return \
        working_output_folder
