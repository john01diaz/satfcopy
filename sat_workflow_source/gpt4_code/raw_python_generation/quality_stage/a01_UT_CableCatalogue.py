import pandas

from sat_workflow_source.b_code.etl_schemas.common.database_names import DatabaseNames
from sat_workflow_source.b_code.etl_schemas.silver_stage.s_cable_catalogue import S_CableCatalogue


def __create_UT_VW_CABLE_CATALOGUE_PART1(
        input_tables):
    cable_dataframe = input_tables['Sigraph.Cable']
    catalogue_dataframe = input_tables['SIgraph_Silver.S_CableCatalogue']
    
    part1_dataframe = pandas.merge(
        cable_dataframe,
        catalogue_dataframe,
        how='left',
        on=[DatabaseNames.DATABASE_NAME.value, S_CableCatalogue.OBJECT_IDENTIFIER.value])
    
    part1_dataframe['UT_ID'] = UT_ID_1
    part1_dataframe['Test_Case'] = part1_dataframe[S_CableCatalogue.OBJECT_IDENTIFIER.value].apply(
        lambda
            x: 'Pass' if pandas.notnull(
            x) else 'Fail')
    
    return part1_dataframe


def __create_UT_VW_CABLE_CATALOGUE_PART2(
        input_tables):
    catalogue_dataframe = input_tables['SIgraph_Silver.S_CableCatalogue']
    
    dup_objects_dataframe = catalogue_dataframe.groupby(
            [DatabaseNames.DATABASE_NAME.value, S_CableCatalogue.OBJECT_IDENTIFIER.value]).filter(
        lambda
            x: len(
            x) > 1)
    
    part2_dataframe = pandas.merge(
        catalogue_dataframe,
        dup_objects_dataframe,
        how='left',
        on=[DatabaseNames.DATABASE_NAME.value, S_CableCatalogue.OBJECT_IDENTIFIER.value])
    
    part2_dataframe['UT_ID'] = UT_ID_2
    part2_dataframe['Test_Case'] = part2_dataframe[S_CableCatalogue.OBJECT_IDENTIFIER.value + '_y'].apply(
        lambda
            x: 'Pass' if pandas.isnull(
            x) else 'Fail')
    
    return part2_dataframe


def __create_UT_VW_CABLE_CATALOGUE_PART3(
        input_tables):
    catalogue_dataframe = input_tables['SIgraph_Silver.S_CableCatalogue']
    
    part3_dataframe = catalogue_dataframe[catalogue_dataframe[S_CableCatalogue.EARTHCORE.value] == 'TRUE'].copy()
    
    part3_dataframe['UT_ID'] = UT_ID_3
    part3_dataframe['Test_Case'] = part3_dataframe[S_CableCatalogue.SIZE.value].apply(
        lambda
            x: 'Fail' if x == FAILURE_SIZE else 'Pass')
    
    return part3_dataframe


def __delete_UNIT_TEST_RESULTS(
        unit_test_results_dataframe):
    unit_test_results_dataframe = unit_test_results_dataframe[unit_test_results_dataframe['Loader_Name'] != LOADER_NAME]
    return unit_test_results_dataframe


def __insert_UNIT_TEST_RESULTS(
        unit_test_results_dataframe,
        ut_vw_cable_catalogue_dataframe):
    ut_vw_cable_catalogue_dataframe['Loader_Name'] = LOADER_NAME
    unit_test_results_dataframe = pandas.concat(
            [unit_test_results_dataframe, ut_vw_cable_catalogue_dataframe])
    return unit_test_results_dataframe


def create_dataframe_silver_quality_01_UT_CableCatalogue_sql_00_00(
        input_tables):
    part1_dataframe = __create_UT_VW_CABLE_CATALOGUE_PART1(
        input_tables)
    part2_dataframe = __create_UT_VW_CABLE_CATALOGUE_PART2(
        input_tables)
    part3_dataframe = __create_UT_VW_CABLE_CATALOGUE_PART3(
        input_tables)
    
    ut_vw_cable_catalogue_dataframe = pandas.concat(
            [part1_dataframe, part2_dataframe, part3_dataframe])
    
    unit_test_results_dataframe = input_tables['SIGRAPH_SILVER.UNIT_TEST_RESULTS']
    
    unit_test_results_dataframe = __delete_UNIT_TEST_RESULTS(
        unit_test_results_dataframe)
    
    unit_test_results_dataframe = __insert_UNIT_TEST_RESULTS(
        unit_test_results_dataframe,
        ut_vw_cable_catalogue_dataframe)
    
    return unit_test_results_dataframe
