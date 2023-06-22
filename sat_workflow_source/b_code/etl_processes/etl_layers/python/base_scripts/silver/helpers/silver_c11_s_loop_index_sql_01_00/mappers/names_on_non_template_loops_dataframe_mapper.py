import pandas
from sat_workflow_source.b_code.model.loop_index_mappings_from_model_getter import get_loop_index_mappings_from_model
from sat_workflow_source.b_code.etl_processes.common.dataframe_columns_renamer import rename_dataframe_columns


def map_names_on_non_template_loops_dataframe(
        column_filtered_non_template_loops_dataframe: pandas.DataFrame) \
        -> pandas.DataFrame:
    mappings = \
        get_loop_index_mappings_from_model()

    loop_index_dataframe = \
        rename_dataframe_columns(
            dataframe=column_filtered_non_template_loops_dataframe,
            mappings=mappings)

    return \
        loop_index_dataframe
