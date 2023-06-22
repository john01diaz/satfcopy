from nf_common_source.code.services.reporting_service.reporters.log_with_datetime import log_message
from pandas import DataFrame


def create_dataframe_gold_c07_loop_index_sql_01_00_v0_02(
        loop_index_dataframe: DataFrame,
        vw_database_names_dataframe: DataFrame) \
        -> DataFrame:
    # TODO: Apparently the input loop_index_dataframe coming from the output of silver stage doesn't have the required
    #  columns: 'database_name', 'Loop_Service_1', but if we read the dataframe from the parquet file, it would have
    #  those columns

    constant_columns = \
        {
            'wired': 'TRUE',
            'drawing': 'TRUE',
            'classname': 'ILP'
        }

    column_filters = \
        [
            'area', 'loopno', 'function', 'number', 'suffix', 'loop_service_1', 'status', 'remarks',
            'formatname', 'areapath', 'loop_function', 'loop_service_2', 'loop_service_3', 'sifpro_relevant',
            'func_description', 'resp_work_center', 'air_distributor', 'var_part_of_drawing_no',
            'classification_by', 'suppl_char_1', 'visual_inspection', 'ep_origin', 'y_coord.', 'et_structure',
            'classification', 'related_report', 'x_coord.', 'field_distrib_output', 'root_extraction_point',
            'suppl_char_3', 'suppl_char_2', 'planning_group', 'tested_by', 'test_acc_to_test_catalog',
            'test_acc_by_test_catalog', 'logic_diag_typical', 'purpose_of_inspection', 'inspection_interval',
            'special_req_3', 'unit', 'accomp_documents', 'special_req_2', 'field_distrib_input',
            'special_req_1',
            'realization_pos', 'function_test', 'graphical_typical', 'standard_loop_id', 'status_remark', 'database_name'] + list(constant_columns.keys())

    column_mappings = \
        {
            'loopno': 'loop no',
            'loop_service_1': 'loop service-1',
            'loop_function': 'loop function',
            'loop_service_2': 'loop service-2',
            'loop_service_3': 'loop service-3',
            'sifpro_relevant': 'sifpro relevant',
            'func_description': 'func description',
            'resp_work_center': 'resp work center',
            'air_distributor': 'air distributor',
            'var_part_of_drawing_no': 'var part of drawing no',
            'classification_by': 'classification by',
            'suppl_char_1': 'suppl char 1',
            'visual_inspection': 'visual inspection',
            'ep_origin': 'ep origin',
            'y_coord.': 'y coord.',
            'et_structure': 'et structure',
            'classification': 'classification',
            'related_report': 'related report',
            'x_coord.': 'x coord.',
            'field_distrib_output': 'field distrib output',
            'root_extraction_point': 'root extraction point',
            'suppl_char_3': 'suppl char 3',
            'suppl_char_2': 'suppl char 2',
            'planning_group': 'planning group',
            'tested_by': 'tested by',
            'test_acc_to_test_catalog': 'test acc to test catalog',
            'test_acc_by_test_catalog': 'test acc by test catalog',
            'logic_diag_typical': 'logic diag typical',
            'purpose_of_inspection': 'purpose of inspection',
            'inspection_interval': 'inspection interval',
            'special_req_3': 'special req 3',
            'unit': 'unit',
            'accomp_documents': 'accomp documents',
            'special_req_2': 'special req 2',
            'field_distrib_input': 'field distrib input',
            'special_req_1': 'special req 1',
            'realization_pos': 'realization pos',
            'function_test': 'function test',
            'graphical_typical': 'graphical typical',
            'standard_loop_id': 'standard loop id',
            'status_remark': 'status remark'
        }

    mapped_dataframe = \
        __map_dataframe_columns(
            dataframe=loop_index_dataframe,
            column_mappings=column_mappings)

    constant_added_dataframe = \
        __add_constant_columns(
            dataframe=mapped_dataframe,
            constant_columns_dictionary=constant_columns)

    filtered_class_dataframe = \
        constant_added_dataframe[constant_added_dataframe['class'] == 'Instrumentation']

    database_names = \
        vw_database_names_dataframe['database_name'].unique()

    column_filters = \
        __update_column_filters_list(
            column_filters=column_filters,
            column_mappings=column_mappings)

    filtered_class_dataframe = \
        __filter_dataframe_by_columns(
            dataframe=filtered_class_dataframe,
            column_names=column_filters)

    final_dataframe = \
        filtered_class_dataframe[
            filtered_class_dataframe['database_name'].isin(database_names)]

    # Note: added by hand because this column is outside scope
    dropped_column_final_dataframe = \
        final_dataframe.drop('database_name', axis=1)

    return \
        dropped_column_final_dataframe.drop_duplicates()


def __filter_dataframe_by_columns(
        dataframe: DataFrame,
        column_names: list) \
        -> DataFrame:
    column_names_aligned_with_dataframe = \
        __get_given_column_names_aligned_with_dataframe(
            column_names=column_names,
            dataframe=dataframe)

    return \
        dataframe[column_names_aligned_with_dataframe]


def __map_dataframe_columns(
        dataframe: DataFrame,
        column_mappings: dict) \
        -> DataFrame:
    column_mappings_filtered_to_dataframe_columns = \
        __get_mapping_source_column_names_aligned_with_dataframe(
            column_mappings=column_mappings,
            dataframe=dataframe)

    return \
        dataframe.rename(
            columns=column_mappings_filtered_to_dataframe_columns)


def __add_constant_columns(
        dataframe: DataFrame,
        constant_columns_dictionary: dict) \
        -> DataFrame:
    for column_name, constant_value \
            in constant_columns_dictionary.items():
        dataframe[column_name] = \
            constant_value

    return \
        dataframe


# Note: it was added because the column_filters values were outdated
def __update_column_filters_list(
        column_filters: list,
        column_mappings: dict) \
        -> list:
    new_column_filters = \
        list()

    for value in column_filters:
        if value in column_mappings.keys():
            new_column_filters.append(
                column_mappings[value])

        else:
            new_column_filters.append(
                value)

    return \
        new_column_filters


# TODO: The following to methods have been added manually for debugging - They should be removed when not needed anymore
def __get_mapping_source_column_names_aligned_with_dataframe(
        column_mappings: dict,
        dataframe: DataFrame) \
        -> dict:
    all_dataframe_column_names = \
        set(dataframe.columns)

    all_mapping_source_column_names = \
        set(column_mappings.keys())

    mapping_source_column_names_not_found_in_dataframe = \
        all_mapping_source_column_names.difference(
            all_dataframe_column_names)

    if mapping_source_column_names_not_found_in_dataframe:
        log_message(
            message='WARNING - mapping source column names not found in source dataframe: '
                    + str(mapping_source_column_names_not_found_in_dataframe))

        for column_name \
                in mapping_source_column_names_not_found_in_dataframe:
            column_mappings.pop(
                column_name)

    return \
        column_mappings


def __get_given_column_names_aligned_with_dataframe(
        column_names: list,
        dataframe: DataFrame) \
        -> list:
    all_dataframe_column_names = \
        set(dataframe.columns)

    all_given_column_names = \
        set(column_names)

    given_column_names_not_found_in_dataframe = \
        all_given_column_names.difference(
            all_dataframe_column_names)

    if given_column_names_not_found_in_dataframe:
        log_message(
            message='WARNING - given column names not found in source dataframe: '
                    + str(given_column_names_not_found_in_dataframe))

        for column_name \
                in given_column_names_not_found_in_dataframe:
            column_names.remove(
                column_name)

    return \
        column_names
