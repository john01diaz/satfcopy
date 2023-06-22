import pandas
from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.silver.helpers.silver_c11_s_loop_index_sql_01_00.columns.cs_loop_unit_components.area_code_from_cs_loop_id_getter import get_area_code_from_cs_loop_id
from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.silver.helpers.silver_c11_s_loop_index_sql_01_00.columns.cs_loop_unit_components.number_from_cs_loop_id_getter import get_number_from_cs_loop_id
from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.silver.helpers.silver_c11_s_loop_index_sql_01_00.columns.cs_loop_unit_components.suffix_from_cs_loop_id_getter import get_suffix_from_cs_loop_id


def add_cs_loop_id_components(
        non_template_loops_dataframe: pandas.DataFrame) \
        -> None:
    non_template_loops_dataframe['area'] = \
        non_template_loops_dataframe.apply(
            lambda x: get_area_code_from_cs_loop_id(x['cs_loop_id']),
            axis=1)

    non_template_loops_dataframe['number'] = \
        non_template_loops_dataframe.apply(
            lambda x: get_number_from_cs_loop_id(x['cs_loop_id']),
            axis=1)

    non_template_loops_dataframe['suffix'] = \
        non_template_loops_dataframe.apply(
            lambda x: get_suffix_from_cs_loop_id(x['cs_loop_id']),
            axis=1)
