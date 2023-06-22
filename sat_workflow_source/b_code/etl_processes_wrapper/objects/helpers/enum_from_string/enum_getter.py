from enum import Enum
from sat_workflow_source.b_code.etl_processes_wrapper.objects.helpers.enum_from_string\
    .enum_from_name_and_root_folder_path_getter import get_enum_from_name_and_root_folder_path
from sat_workflow_source.b_code.etl_processes_wrapper.objects.helpers.enum_from_string.enum_root_folder_path_getter \
    import get_enum_root_folder_path


def get_enum(
        enum_name: str) \
        -> Enum:
    enum_root_folder_path = \
        get_enum_root_folder_path()
    
    enum = \
        get_enum_from_name_and_root_folder_path(
            enum_name=enum_name,
            root_folder_path=enum_root_folder_path)

    return \
        enum
