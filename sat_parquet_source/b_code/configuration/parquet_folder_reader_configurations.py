from nf_common_source.code.services.file_system_service.objects.folders import Folders


class ParquetFolderReaderConfigurations:
    def __init__(
            self,
            input_root_folder: Folders,
            output_root_folder: Folders,
            output_folder_prefix: str,
            input_root_folder_name: str):
        self.input_root_folder = \
            input_root_folder

        self.output_root_folder = \
            output_root_folder

        self.output_folder_prefix = \
            output_folder_prefix

        self.input_root_folder_name = \
            input_root_folder_name
