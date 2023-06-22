import pandas

from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.silver.helpers.silver_c11_s_loop_index_sql_01_00.finalizers.s_loop_index_from_loop_index_creator import create_s_loop_index_from_loop_index
from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.silver.helpers.silver_c11_s_loop_index_sql_01_00.tables.layer_loop_pbs_dataframe_getter import get_layer_loop_pbs_dataframe
from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.silver.helpers.silver_c11_s_loop_index_sql_01_00.filters.to_non_template_loops_filterer import filter_to_non_template_loops
from sat_workflow_source.b_code.model.loop_picklist_from_model_getter import get_loop_picklist_from_model
from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.silver.helpers.silver_c11_s_loop_index_sql_01_00.columns.complex_mappings.complex_columns_to_non_template_loops_dataframe_adder import add_complex_columns_to_non_template_loops_dataframe
from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.silver.helpers.silver_c11_s_loop_index_sql_01_00.filters.non_template_loops_columns_filterer import filter_non_template_loops_columns
from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.silver.helpers.silver_c11_s_loop_index_sql_01_00.mappers.names_on_non_template_loops_dataframe_mapper import map_names_on_non_template_loops_dataframe


def create_silver_c11_s_loop_index_sql_01_00_dataframe(
        layer_dataframe: pandas.DataFrame,
        loop_dataframe: pandas.DataFrame,
        pbs_dataframe: pandas.DataFrame) \
        -> pandas.DataFrame:
    layer_loop_pbs_dataframe = \
        get_layer_loop_pbs_dataframe(
            layer_dataframe=layer_dataframe,
            loop_dataframe=loop_dataframe,
            pbs_dataframe=pbs_dataframe)

    non_template_loops_dataframe = \
        filter_to_non_template_loops(
            layer_loop_pbs_dataframe=layer_loop_pbs_dataframe)

    loop_picklist = \
        get_loop_picklist_from_model()

    add_complex_columns_to_non_template_loops_dataframe(
        non_template_loops_dataframe=non_template_loops_dataframe,
        loop_picklist=loop_picklist)

    column_filtered_non_template_loops_dataframe = \
        filter_non_template_loops_columns(
            non_template_loops_dataframe=non_template_loops_dataframe)

    loop_index_dataframe = \
        map_names_on_non_template_loops_dataframe(
            column_filtered_non_template_loops_dataframe=column_filtered_non_template_loops_dataframe)

    s_loop_index_table = \
        create_s_loop_index_from_loop_index(
            loop_index_dataframe=loop_index_dataframe)

    return \
        s_loop_index_table
