import pandas


def add_loop_no(
        non_template_loops_dataframe: pandas.DataFrame) \
        -> None:
    # case when
    #         CS_loop_id like '%*'  or CS_loop_id like '%?'  or CS_loop_id like '%~'
    #         then translate(CS_loop_id,"*?~","")
    #         else CS_loop_id
    non_template_loops_dataframe['loopno'] = \
        ''
