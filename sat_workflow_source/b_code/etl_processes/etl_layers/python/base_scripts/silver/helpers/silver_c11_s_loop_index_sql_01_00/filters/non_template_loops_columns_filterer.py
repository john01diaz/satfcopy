import pandas
from sat_workflow_source.b_code.model.loop_index_columns_from_model_getter import get_loop_index_columns_from_model


def filter_non_template_loops_columns(
        non_template_loops_dataframe: pandas.DataFrame) \
        -> pandas.DataFrame:
    copy_of_filtered_non_template_loops_dataframe = \
        non_template_loops_dataframe.copy(
            deep=True)

    # todo: get from enum list
    columns_to_keep = \
        get_loop_index_columns_from_model()

    column_filtered_non_template_loops_dataframe = \
        copy_of_filtered_non_template_loops_dataframe.filter(
            items=columns_to_keep)

    return \
        column_filtered_non_template_loops_dataframe
