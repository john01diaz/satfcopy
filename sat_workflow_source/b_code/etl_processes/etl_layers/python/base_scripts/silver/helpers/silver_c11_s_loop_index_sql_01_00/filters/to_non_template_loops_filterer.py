import pandas


def filter_to_non_template_loops(
        layer_loop_pbs_dataframe: pandas.DataFrame) \
        -> pandas.DataFrame:
    return \
        layer_loop_pbs_dataframe.copy(
            deep=True)
