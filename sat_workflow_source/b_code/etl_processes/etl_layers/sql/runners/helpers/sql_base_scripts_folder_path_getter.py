import os
from sat_workflow_source.b_code.etl_processes.etl_layers.sql.runners.helpers.git_root_path_getter import \
    get_git_root_path


def get_sql_base_scripts_folder_path() \
        -> str:
    sql_base_scripts_folder_path = \
        os.path.join(
            get_git_root_path(),
            'sat_workflow_source',
            'b_code',
            'etl_processes',
            'etl_layers',
            'sql',
            'base_scripts')

    return \
        sql_base_scripts_folder_path
