import pandas
from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.silver.helpers.silver_c11_s_loop_index_sql_01_00.columns.complex_mappings.area_path_adder import add_area_path
from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.silver.helpers.silver_c11_s_loop_index_sql_01_00.columns.complex_mappings.cs_loop_id_components_adder import add_cs_loop_id_components
from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.silver.helpers.silver_c11_s_loop_index_sql_01_00.columns.complex_mappings.format_name_adder import add_format_name
from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.silver.helpers.silver_c11_s_loop_index_sql_01_00.columns.complex_mappings.loop_no_adder import add_loop_no
from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.silver.helpers.silver_c11_s_loop_index_sql_01_00.columns.complex_mappings.loop_picklist_mappings_adder import add_loop_picklist_mappings
from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.silver.helpers.silver_c11_s_loop_index_sql_01_00.columns.instrumentation_or_electrical_getter import get_instrumentation_or_electrical
from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.silver.helpers.silver_c11_s_loop_index_sql_01_00.columns.characters_remover import remove_characters


def add_complex_columns_to_non_template_loops_dataframe(
        non_template_loops_dataframe: pandas.DataFrame,
        loop_picklist: dict) \
        -> None:
    add_loop_no(
        non_template_loops_dataframe=non_template_loops_dataframe)

    add_cs_loop_id_components(
        non_template_loops_dataframe=non_template_loops_dataframe)

    add_format_name(
        non_template_loops_dataframe=non_template_loops_dataframe)

    add_area_path(
        non_template_loops_dataframe=non_template_loops_dataframe)

    non_template_loops_dataframe['loop_service_1'] = \
        non_template_loops_dataframe.apply(
            lambda x: remove_characters(x['cs_loop_description_1'], '?#*'),
            axis=1)

    non_template_loops_dataframe['class'] = \
        non_template_loops_dataframe.apply(
            lambda x: get_instrumentation_or_electrical(x['loop_dynamic_class']),
            axis=1)

    add_loop_picklist_mappings(
        non_template_loops_dataframe=non_template_loops_dataframe,
        loop_picklist=loop_picklist)
