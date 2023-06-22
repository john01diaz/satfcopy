import pandas


def add_area_path(
        non_template_loops_dataframe: pandas.DataFrame) \
        -> None:
    # concat_ws('-',Site_Code,Coalesce(Revised_Plant_Code,Plant_Code),Coalesce(Revised_Process_Unit,Process_Unit))
    non_template_loops_dataframe['areapath'] = \
        ''
