import pandas


def create_silver_c11_s_loop_index_sql_01_00_dataframe_gpt_1(
        layer_dataframe: pandas.DataFrame,
        loop_dataframe: pandas.DataFrame,
        pbs_dataframe: pandas.DataFrame) \
        -> pandas.DataFrame:
    # Join the necessary tables first
    merged_data = layer_dataframe.merge(loop_dataframe, left_on=['database_name', 'object_identifier', 'dynamic_class'],
                                        right_on=['loop_database_name', 'layer_cs_loop_href',
                                                  'layer_cs_loop_dyn_class'])
    # Join the necessary tables first
    merged_data = merged_data.merge(pbs_dataframe, left_on=[
        merged_data['name'].apply(
            lambda x: x.replace('RAUM_', '')[:100] if x.startswith('RAUM') else x.split('_')[1] if '_' in x else
            x.split('_')[0])], right_on=['area'])

    # Drop the unnecessary columns
    merged_data = merged_data.drop(['area'], axis=1)

    # merged_data = merged_data.merge(pbs_dataframe, left_on=[
    #     merged_data['name'].str.replace('RAUM_', '').str[:100].where(merged_data['name'].str.startswith('RAUM'),
    #                                                                  other=merged_data['name'].str.extract(
    #                                                                      r'^(.*?)_.*?$', expand=False)).where(
    #         merged_data['name'].str.extract(r'^(.*?)_.*?$', expand=False) == merged_data['loop_cs_loop_unit'],
    #         other=merged_data['name'].str.extract(r'^.*?_(.*?)$', expand=False)).where(
    #         merged_data['name'].str.extract(r'^(.*?)_.*?$', expand=False),
    #         other=merged_data['name'].str.extract(r'^(.*?)_.*?$', expand=False))], right_on='area_code')

    # Create the loop_index dataframe
    loop_index = pandas.DataFrame()

    # Select distinct columns and assign values
    loop_index['loop_database_name'] = merged_data['loop_database_name']
    loop_index['loop_dynamic_class'] = merged_data['loop_dynamic_class']
    loop_index['loop_object_identifier'] = merged_data['loop_object_identifier']
    loop_index['name'] = merged_data['name']
    loop_index['loopno'] = "tobedone"
    loop_index['area'] = merged_data['cs_loop_id'].str.extract(r'^([0-9]+)')
    loop_index['number'] = merged_data['cs_loop_id'].str.extract(r'^([0-9]+)([A-Z]+)([0-9]+)')[2]
    loop_index['suffix'] = merged_data['cs_loop_id'].str.extract(r'^([0-9]+)([A-Z]+)([0-9]+)([-_ ]*)([A-Z0-9]*$)')[4]
    loop_index['formatname'] = "tobedone"

    # Plant breakdown structure columns
    loop_index['areapath'] = "tobedone"

    # Remaining columns
    loop_index['function'] = merged_data['cs_loop_kat_id']
    loop_index['remarks'] = merged_data['loop_cs_comment']
    loop_index['loop_service_1'] = merged_data['cs_loop_description_1'].str.replace('[?#*]', '')
    loop_index['func_description'] = merged_data['cs_function_description']
    loop_index['resp_work_center'] = merged_data['sap_responsible_work_center']
    loop_index['air_distributor'] = merged_data['cs_air_distributor']
    loop_index['var_part_of_drawing_no'] = merged_data['cs_variable_part_drawing_number']
    loop_index['suppl_char_1'] = merged_data['cs_loop_id_1']
    loop_index['visual_inspection'] = merged_data['cs_visual_inspection']
    loop_index['y_coord.'] = merged_data['cs_y_coordinate_pi_data']
    loop_index['et_structure'] = merged_data['cs_et_node_path']
    loop_index['x_coord.'] = merged_data['cs_x_coordinate_pi_data']
    loop_index['field_distrib_output'] = merged_data['cs_field_distributor_output_id']
    loop_index['suppl_char_3'] = merged_data['cs_loop_id_3']
    loop_index['suppl_char_2'] = merged_data['cs_loop_id_2']
    loop_index['planning_group'] = merged_data['sap_planning_group']
    loop_index['tested_by'] = merged_data['cs_tested_by']
    loop_index['test_acc_to_test_catalog'] = merged_data['cs_test_acc_to_test_catalog']
    loop_index['test_acc_by_test_catalog'] = merged_data['cs_test_acc_by_test_catalog']
    loop_index['logic_diag_typical'] = merged_data['cs_logic_diagram_typical']
    loop_index['purpose_of_inspection'] = merged_data['cs_inspection_purpose']
    loop_index['inspection_interval'] = merged_data['cs_inspection_interval']
    loop_index['unit'] = merged_data['loop_cs_loop_unit']
    loop_index['accomp_documents'] = merged_data['cs_accompanying_documents']
    loop_index['field_distrib_input'] = merged_data['cs_field_distributor_input_id']
    loop_index['realization_pos'] = merged_data['cs_realization_position']
    loop_index['function_test'] = merged_data['cs_function_test']
    loop_index['loop_service_3'] = merged_data['cs_loop_description_3']
    loop_index['graphical_typical'] = merged_data['cs_graphic_typical']
    loop_index['standard_loop_id'] = merged_data['cs_standard_loop_id']
    loop_index['loop_service_2'] = merged_data['cs_loop_description_2']
    loop_index['status_remark'] = merged_data['cs_loop_status_remark']
    loop_index['loop_no'] = merged_data['cs_loop_lfd_nr']
    loop_index['loop_function'] = merged_data['cs_loop_function']
    loop_index['class'] = merged_data['loop_dynamic_class'].apply(
        lambda x: 'Instrumentation' if x == 'CS_Loop_spez' else 'Electrical')

    # Picklist Property
    loop_index['status'] = "tobedone"
    loop_index['ep_origin'] = "tobedone"
    loop_index['classification'] = "tobedone"
    loop_index['related_report'] = "tobedone"
    loop_index['root_extraction_point'] = "tobedone"
    loop_index['special_req_1'] = "tobedone"
    loop_index['special_req_2'] = "tobedone"
    loop_index['special_req_3'] = "tobedone"
    loop_index['classification_by'] = "tobedone"
    loop_index['sifpro_relevant'] = "tobedone"

    # Apply the filtering condition
    loop_index = loop_index[loop_index['template_loop'] == "FALSE"]

    return \
        loop_index
