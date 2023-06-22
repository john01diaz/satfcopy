import importlib.util
import inspect
import os


def get_code_process_from_name_and_root_folder_path(
        code_process_name: str,
        root_folder_path: str):
    for path, subdirs, filenames in os.walk(root_folder_path):
        for filename in filenames:
            code_process = \
                get_code_process_or_none(
                    path,
                    filename,
                    code_process_name)
            
            if code_process:
                return \
                    code_process


def get_code_process_or_none(
        path,
        filename,
        code_process_name):
    if not filename.endswith(".py"):
        return
        
    module_name = \
        filename[:-3]
    
    file_path = \
        os.path.join(
            path,
            filename)

    module_specification = \
        importlib.util.spec_from_file_location(
            module_name,
            file_path)

    module = \
        importlib.util.module_from_spec(
            module_specification)

    module_specification.loader.exec_module(
        module)

    for name, function in inspect.getmembers(
            module):
        if inspect.isfunction(
                function):
            if name == code_process_name:
                function = getattr(
                    module,
                    name)
                
                return \
                    function
