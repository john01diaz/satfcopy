import glob


def get_folder_children_paths(
        stage_name: str,
        parquet_silver_root_folder_path: str) \
        -> list:
    stage_folder_child_stage_folder_path_children = \
        glob.glob(
            parquet_silver_root_folder_path + "/" + stage_name + "/*",
            recursive=True)

    return \
        stage_folder_child_stage_folder_path_children
