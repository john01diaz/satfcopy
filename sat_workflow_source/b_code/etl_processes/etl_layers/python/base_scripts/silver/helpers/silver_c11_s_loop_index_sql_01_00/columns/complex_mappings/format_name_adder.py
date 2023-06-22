import pandas


def add_format_name(
        non_template_loops_dataframe: pandas.DataFrame) \
        -> None:
    # to_get_loop_format_tag(CS_loop_id)
    non_template_loops_dataframe['formatname'] = \
        ''
