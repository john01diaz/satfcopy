import os


def get_etl_layer_root_folder_path(
        type_folder_name: str) \
        -> str:
    file_path = \
        os.path.dirname(
            os.path.abspath(__file__))
    
    project_root_position = \
        file_path.find(
            os.sep + r'sat_workflow_source')
    
    etl_layer_root_folder_path = \
        os.path.join(
            file_path[:project_root_position],
            'sat_workflow_source',
            'b_code',
            'etl_processes',
            'etl_layers',
            type_folder_name)
    
    return \
        etl_layer_root_folder_path
