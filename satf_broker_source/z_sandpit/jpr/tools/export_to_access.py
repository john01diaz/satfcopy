# import os
#
# from nf_common_source.code.services.file_system_service.folder_selector import select_folder
# from nf_common_source.code.services.file_system_service.objects.folders import Folders
# from nf_common_source.code.services.input_output_service.access.all_xlsx_files_from_folder_to_access_exporter import \
#     export_all_xlsx_files_from_folder_to_access
# from nf_common_source.code.services.reporting_service.reporters.log_file import LogFiles
#
# xlsx_folder = select_folder()
#
# LogFiles.open_log_file(xlsx_folder.absolute_path_string)
#
# export_all_xlsx_files_from_folder_to_access(
#     xlsx_folder=xlsx_folder,
#     database_already_exists=False,
#     new_database_name_if_not_exists='test')
