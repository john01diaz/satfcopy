import pandas
from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.silver.helpers.silver_c11_s_loop_index_sql_01_00.columns.sigraph_to_new_name_converter import convert_sigraph_to_new_name


def add_loop_picklist_mappings(
        non_template_loops_dataframe: pandas.DataFrame,
        loop_picklist: dict) \
        -> pandas.DataFrame:
    non_template_loops_dataframe['status'] = \
        non_template_loops_dataframe.apply(
            lambda x: convert_sigraph_to_new_name('cs_loop_status', x['cs_loop_status'], loop_picklist, 'BEING PLANNED'),
            axis=1)

    non_template_loops_dataframe['ep_origin'] = \
        non_template_loops_dataframe.apply(
            lambda x: convert_sigraph_to_new_name('cs_source', x['cs_source'], loop_picklist),
            axis=1)

    non_template_loops_dataframe['classification'] = \
        non_template_loops_dataframe.apply(
            lambda x: convert_sigraph_to_new_name('loop_cs_classification', x['loop_cs_classification'], loop_picklist),
            axis=1)

    non_template_loops_dataframe['related_report'] = \
        non_template_loops_dataframe.apply(
            lambda x: convert_sigraph_to_new_name('cs_related_report', x['cs_related_report'], loop_picklist),
            axis=1)

    non_template_loops_dataframe['root_extraction_point'] = \
        non_template_loops_dataframe.apply(
            lambda x: convert_sigraph_to_new_name('cs_root_extraction_point', x['cs_root_extraction_point'], loop_picklist),
            axis=1)

    non_template_loops_dataframe['special_req_1'] = \
        non_template_loops_dataframe.apply(
            lambda x: convert_sigraph_to_new_name('cs_special_requirements_1', x['cs_special_requirements_1'], loop_picklist),
            axis=1)

    non_template_loops_dataframe['special_req_2'] = \
        non_template_loops_dataframe.apply(
            lambda x: convert_sigraph_to_new_name('cs_special_requirements_2', x['cs_special_requirements_2'], loop_picklist),
            axis=1)

    non_template_loops_dataframe['special_req_3'] = \
        non_template_loops_dataframe.apply(
            lambda x: convert_sigraph_to_new_name('cs_special_requirements_3', x['cs_special_requirements_3'], loop_picklist),
            axis=1)

    non_template_loops_dataframe['classification_by'] = \
        non_template_loops_dataframe.apply(
            lambda x: convert_sigraph_to_new_name('cs_special_requirements_3', x['loop_cs_classification_by'], loop_picklist),
            axis=1)

    non_template_loops_dataframe['sifpro_relevant'] = \
        non_template_loops_dataframe.apply(
            lambda x: convert_sigraph_to_new_name('cs_special_requirements_3', x['cs_sifpro_relevant'], loop_picklist),
            axis=1)

    return \
        non_template_loops_dataframe
