import pandas
from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.silver.helpers.silver_c11_s_loop_index_sql_01_00.columns.process_unit_from_name_getter import get_process_unit_from_name
from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.silver.helpers.silver_c11_s_loop_index_sql_01_00.columns.cs_loop_unit_components.area_code_from_cs_loop_id_getter import get_area_code_from_cs_loop_id


def get_layer_loop_pbs_dataframe(
        layer_dataframe: pandas.DataFrame,
        loop_dataframe: pandas.DataFrame,
        pbs_dataframe: pandas.DataFrame) \
        -> pandas.DataFrame:
    layer_loop_dataframe = \
        layer_dataframe.merge(
            loop_dataframe,
            how='inner',
            left_on=['database_name', 'object_identifier', 'dynamic_class'],
            right_on=['loop_database_name', 'layer_cs_loop_href', 'layer_cs_loop_dyn_class'])

    __add_additional_columns(
        layer_loop_dataframe=layer_loop_dataframe)

    layer_loop_pbs_dataframe = \
        layer_loop_dataframe.merge(
            pbs_dataframe,
            how='inner',
            left_on=['process_unit_from_name', 'area_code_from_cs_loop_id'],
            right_on=['process_unit', 'area_code'])

    return \
        layer_loop_pbs_dataframe


def __add_additional_columns(
        layer_loop_dataframe: pandas.DataFrame) \
        -> None:
    layer_loop_dataframe['process_unit_from_name'] = \
        layer_loop_dataframe.apply(
            lambda x: get_process_unit_from_name(x['name'], x['cs_loop_unit']),
            axis=1)

    layer_loop_dataframe['area_code_from_cs_loop_id'] = \
        layer_loop_dataframe.apply(
            lambda x: get_area_code_from_cs_loop_id(x['cs_loop_unit']),
            axis=1)
