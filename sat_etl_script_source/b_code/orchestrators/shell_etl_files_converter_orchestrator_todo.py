# import os
# import shutil
# from nf_common_source.code.services.file_system_service.objects.folders import Folders
# from nf_common_source.code.services.reporting_service.reporters.log_file import LogFiles
# from nf_common_source.code.services.reporting_service.reporters.log_with_datetime import log_message
# from nf_common_source.code.services.reporting_service.wrappers.run_and_log_function_wrapper import run_and_log_function
#
#
# @run_and_log_function
# def orchestrate_shell_etl_files_converter_stage_02(
#         input_root_folder: Folders) \
#         -> None:
#     log_message(
#         message='input filepath:' + input_root_folder.absolute_path_string)
#
#     relative_path_removal = \
#         input_root_folder.parent_absolute_path_string + os.sep
#
#     sql_extension_folder_path = \
#         os.path.join(
#             LogFiles.folder_path,
#             'stage_01',
#             'sql_extension')
#
#     for input_folder_path, dirs, filenames \
#             in os.walk(sql_extension_folder_path):
#         relative_path = \
#             input_folder_path.replace(relative_path_removal,'')
#
#         log_message(
#             message='processing folder:  ' + relative_path)
#
#         output_sql_extension_folder_path = \
#             os.path.join(
#                 sql_extension_folder_path,
#                 relative_path)
#
#         output_python_extension_folder_path = \
#             os.path.join(
#                 python_extension_folder_path,
#                 relative_path)
#
#         os.mkdir(
#             output_sql_extension_folder_path)
#
#         for filename \
#                 in filenames:
#             __process_child_file(
#                 filename=filename,
#                 input_folder_path=input_folder_path,
#                 output_sql_extension_folder_path=output_sql_extension_folder_path,
#                 output_python_extension_folder_path=output_python_extension_folder_path)
#
#
# def __process_child_file(
#         filename: str,
#         input_folder_path: str,
#         output_sql_extension_folder_path: str,
#         output_python_extension_folder_path: str) \
#         -> None:
#     log_message(
#         message='processing file:      ' + filename)
#
#     input_file_path = \
#         os.path.join(
#             input_folder_path,
#             filename)
#
#     if filename[-4:] == '.sql':
#         __process_sql_file(
#                 filename=filename,
#                 input_file_path=input_file_path,
#                 output_folder_path=output_sql_extension_folder_path)
#
#     else:
#         output_file_path = \
#             os.path.join(
#                 output_python_extension_folder_path,
#                 filename)
#
#         shutil.copyfile(
#             input_file_path,
#             output_python_extension_folder_path)
#
#
# def __process_sql_file(
#         filename: str,
#         input_file_path: str,
#         output_folder_path: str) \
#         -> None:
#     with open(input_file_path, 'r') as f:
#         commands = f.read().split('-- COMMAND ----------\n')
#
#     command_position = 0
#
#     for command in commands:
#         extention = 'sql'
#
#         is_magic = False
#
#         if command.find('-- MAGIC') > 0:
#             is_magic = True
#
#         magic_type = 'not_found'
#
#         if is_magic:
#             if command.find('%run') > 0:
#                 magic_type = 'run'
#                 extention = 'run'
#
#             if command.find('%sql') > 0:
#                 magic_type = 'sql'
#
#             if command.find('%python') > 0:
#                 magic_type = 'python'
#                 extention = 'py'
#
#         new_filename = \
#             filename[:-4] + '_sql_' + f'{command_position:02}'
#
#         if is_magic:
#             new_filename = new_filename + '_magic'
#
#         if magic_type == 'sql':
#             command = command.replace('-- MAGIC ','')
#
#             command = command.replace('%sql','')
#
#         if extention == 'sql':
#             sql_commands = command.split(';')
#
#             if len(sql_commands) > 1:
#                 e=0
#
#             sql_command_position = 0
#
#             for sql_command in sql_commands:
#                 output_file_path = \
#                     os.path.join(
#                         output_folder_path,
#                         new_filename + '_' + f'{sql_command_position:02}' + '.' + extention)
#
#                 with open(output_file_path, 'w') as f:
#                     f.write(sql_command)
#
#                 sql_command_position += \
#                     1
#
#         else:
#             output_file_path = \
#                 os.path.join(
#                     output_folder_path,
#                     new_filename + '.' + extention)
#
#             with open(output_file_path, 'w') as f:
#                 f.write(command)
#
#         command_position += \
#             1
