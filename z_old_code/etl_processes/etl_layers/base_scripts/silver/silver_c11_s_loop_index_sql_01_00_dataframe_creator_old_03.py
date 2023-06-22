import pandas


def create_silver_c11_s_loop_index_sql_01_00_dataframe_gpt_2(
        layer_dataframe: pandas.DataFrame,
        loop_dataframe: pandas.DataFrame,
        pbs_dataframe: pandas.DataFrame) \
        -> pandas.DataFrame:
    import pandas as pd

    # Perform JOIN operations
    joined_df = loop_dataframe.merge(layer_dataframe, left_on=['loop_database_name', 'loop_dynamic_class', 'loop_object_identifier'],
                              right_on=['database_name', 'dynamic_class', 'object_identifier'], how='inner')
    joined_df = joined_df.merge(pbs_dataframe, left_on=['name'], right_on=['distinct_column'], how='inner')
    joined_df = joined_df[joined_df['template_loop'] == 'FALSE']

    # Define constants
    AREA_REGEX = r'^[0-9]+'
    NUMBER_REGEX = r'(^[0-9]+)([A-Z]+)([0-9]+)'
    SUFFIX_REGEX = r'(^[0-9]+)([A-Z]+)([0-9]+)([-_ ]*)([A-Z0-9]*$)'

    # Perform column transformations and mapping
    loop_index = pd.DataFrame()
    loop_index['loop_database_name'] = joined_df['loop_database_name']
    loop_index['loop_dynamic_class'] = joined_df['loop_dynamic_class']
    loop_index['loop_object_identifier'] = joined_df['loop_object_identifier']
    loop_index['name'] = joined_df['name']
    loop_index['loopno'] = 'tobedone'
    loop_index['area'] = joined_df['cs_loop_id'].str.extract(AREA_REGEX, expand=False)
    loop_index['number'] = joined_df['cs_loop_id'].str.extract(NUMBER_REGEX, expand=3)
    loop_index['suffix'] = joined_df['cs_loop_id'].str.extract(SUFFIX_REGEX, expand=5)
    loop_index['formatname'] = 'tobedone'
    loop_index['areapath'] = 'tobedone'
    loop_index['function'] = joined_df['cs_loop_kat_id']
    loop_index['remarks'] = joined_df['loop_cs_comment']
    loop_index['loop_service_1'] = joined_df['cs_loop_description_1'].str.replace('[?#*]', '')
    loop_index['func_description'] = joined_df['cs_function_description']
    loop_index['resp_work_center'] = joined_df['sap_responsible_work_center']
    loop_index['air_distributor'] = joined_df['cs_air_distributor']
    loop_index['var_part_of_drawing_no'] = joined_df['cs_variable_part_drawing_number']
    loop_index['suppl_char_1'] = joined_df['cs_loop_id_1']
    loop_index['visual_inspection'] = joined_df['cs_visual_inspection']
    loop_index['y_coord'] = joined_df['cs_y_coordinate_pi_data']
    loop_index['et_structure'] = joined_df['cs_et_node_path']
    loop_index['x_coord'] = joined_df['cs_x_coordinate_pi_data']
    loop_index['field_distrib_output'] = joined_df['cs_field_distributor_output_id']
    loop_index['suppl_char_3'] = joined_df['cs_loop_id_3']
    loop_index['suppl_char_2'] = joined_df['cs_loop_id_2']
    loop_index['planning_group'] = joined_df['sap_planning_group']
    loop_index['tested_by'] = joined_df['cs_tested_by']
    loop_index['test_acc_to_test_catalog'] = joined_df['cs_test_acc_to_test_catalog']
    loop_index['test_acc_by_test_catalog'] = joined_df['cs_test_acc_by_test_catalog']
    loop_index['logic_diag_typical'] = joined_df['cs_logic_diagram_typical']
    loop_index['purpose_of_inspection'] = joined_df['cs_inspection_purpose']
    loop_index['inspection_interval'] = joined_df['cs_inspection_interval']
    loop_index['unit'] = joined_df['loop_cs_loop_unit']
    loop_index['accomp_documents'] = joined_df['cs_accompanying_documents']
    loop_index['field_distrib_input'] = joined_df['cs_field_distributor_input_id']
    loop_index['realization_pos'] = joined_df['cs_realization_position']
    loop_index['function_test'] = joined_df['cs_function_test']
    loop_index['loop_service_3'] = joined_df['cs_loop_description_3']
    loop_index['graphical_typical'] = joined_df['cs_graphic_typical']
    loop_index['standard_loop_id'] = joined_df['cs_standard_loop_id']
    loop_index['loop_service_2'] = joined_df['cs_loop_description_2']
    loop_index['status_remark'] = joined_df['cs_loop_status_remark']
    loop_index['loop_no'] = joined_df['cs_loop_lfd_nr']
    loop_index['loop_function'] = joined_df['cs_loop_function']
    loop_index['class'] = joined_df['loop_dynamic_class'].map(
        {'CS_Loop_spez': 'Instrumentation', 'default': 'Electrical'})

    # Additional columns (picklist properties)
    loop_index['status'] = 'tobedone'
    loop_index['ep_origin'] = 'tobedone'
    loop_index['classification'] = 'tobedone'
    loop_index['related_report'] = 'tobedone'
    loop_index['root_extraction_point'] = 'tobedone'
    loop_index['special_req_1'] = 'tobedone'
    loop_index['special_req_2'] = 'tobedone'
    loop_index['special_req_3'] = 'tobedone'
    loop_index['classification_by'] = 'tobedone'
    loop_index['sifpro_relevant'] = 'tobedone'

    return \
        loop_index
