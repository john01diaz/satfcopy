import os


def get_enum_root_folder_path() \
        -> str:
    file_path = \
        os.path.dirname(
            os.path.abspath(__file__))
    
    project_root_position = \
        file_path.find(
            os.sep + r'sat_workflow_source')
    
    base_script_root_folder_path = \
        os.path.join(
            file_path[:project_root_position],
            'sat_workflow_source',
            'b_code',
            'etl_schemas')
    
    return \
        base_script_root_folder_path
