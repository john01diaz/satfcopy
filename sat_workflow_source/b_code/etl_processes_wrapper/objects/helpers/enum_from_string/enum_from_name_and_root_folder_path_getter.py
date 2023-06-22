import importlib.util
import inspect
import os


def get_enum_from_name_and_root_folder_path(
        enum_name: str,
        root_folder_path: str):
    for path, subdirectories, filenames in os.walk(root_folder_path):
        for filename in filenames:
            enum = \
                get_enum_or_none(
                    path,
                    filename,
                    enum_name)
            
            if enum:
                return \
                    enum


def get_enum_or_none(
        path,
        filename,
        enum_name):
    if not filename.endswith('.py'):
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

    classes = \
        inspect.getmembers(
            module,
            inspect.isclass)

    class_dict = \
        dict(
            classes)

    if enum_name in class_dict.keys():
        enum = \
            class_dict[enum_name]
    
        return \
            enum

    return
