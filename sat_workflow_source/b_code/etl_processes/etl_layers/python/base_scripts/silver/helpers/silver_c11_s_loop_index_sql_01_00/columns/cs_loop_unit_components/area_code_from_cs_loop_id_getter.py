from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.silver.helpers.silver_c11_s_loop_index_sql_01_00.columns.cs_loop_unit_components.regex_match_from_cs_loop_id_getter import get_regex_match_from_input_string


def get_area_code_from_cs_loop_id(
        cs_loop_id: str) \
        -> str:
    area_code = \
        get_regex_match_from_input_string(
            input_string=cs_loop_id,
            regex=r'^[0-9]+')

    return \
        area_code
