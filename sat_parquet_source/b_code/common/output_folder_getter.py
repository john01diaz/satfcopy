from nf_common_source.code.services.file_system_service.objects.folders import Folders
from nf_common_source.code.services.reporting_service.reporters.log_file import LogFiles


def get_output_folder():
    output_folder = \
        Folders(
            absolute_path_string=LogFiles.folder_path)

    return \
        output_folder
