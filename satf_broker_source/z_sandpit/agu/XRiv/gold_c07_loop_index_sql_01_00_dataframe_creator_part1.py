from sat_workflow_source.b_code.etl_schemas.common.database_names import DatabaseNames
from sat_workflow_source.b_code.etl_schemas.gold_stage.loop_index import Loop_Index
from sat_workflow_source.b_code.etl_schemas.silver_stage.s_loop_index import S_LoopIndex


def create_dataframe_gold_c07_loop_index_sql_01_00_v0_02(
        loop_index_dataframe,
        database_names_dataframe):
    # Defining the simple column mappings
    simple_column_mapping = {
        S_LoopIndex.AREA.value                  : Loop_Index.AREA.value,
        S_LoopIndex.LOOPNO.value                : Loop_Index.LOOP_NO.value,
        S_LoopIndex.FUNCTION.value              : Loop_Index.FUNCTION.value,
        S_LoopIndex.NUMBER.value                : Loop_Index.NUMBER.value,
        S_LoopIndex.SUFFIX.value                : Loop_Index.SUFFIX.value,
        S_LoopIndex.LOOP_SERVICE_1.value        : Loop_Index.LOOP_SERVICE_1.value,
        S_LoopIndex.STATUS.value                : Loop_Index.STATUS.value,
        S_LoopIndex.REMARKS.value               : Loop_Index.REMARKS.value,
        S_LoopIndex.FORMATNAME.value            : Loop_Index.FORMATNAME.value,
        S_LoopIndex.AREAPATH.value              : Loop_Index.AREAPATH.value,
        S_LoopIndex.LOOP_FUNCTION.value         : Loop_Index.LOOP_FUNCTION.value,
        S_LoopIndex.LOOP_SERVICE_2.value        : Loop_Index.LOOP_SERVICE_2.value,
        S_LoopIndex.LOOP_SERVICE_3.value        : Loop_Index.LOOP_SERVICE_3.value,
        S_LoopIndex.SIFPRO_RELEVANT.value       : Loop_Index.SIFPRO_RELEVANT.value,
        S_LoopIndex.FUNC_DESCRIPTION.value      : Loop_Index.FUNC_DESCRIPTION.value,
        S_LoopIndex.RESP_WORK_CENTER.value      : Loop_Index.RESP_WORK_CENTER.value,
        S_LoopIndex.AIR_DISTRIBUTOR.value       : Loop_Index.AIR_DISTRIBUTOR.value,
        S_LoopIndex.VAR_PART_OF_DRAWING_NO.value: Loop_Index.VAR_PART_OF_DRAWING_NO.value,
        S_LoopIndex.CLASSIFICATION_BY.value     : Loop_Index.CLASSIFICATION_BY.value,
        S_LoopIndex.SUPPL_CHAR_1.value          : Loop_Index.SUPPL_CHAR_1.value,
        S_LoopIndex.VISUAL_INSPECTION.value     : Loop_Index.VISUAL_INSPECTION.value,
        }
    
    # Defining the constant value assignments
    constant_value_assignments = {
        Loop_Index.WIRED.value    : 'TRUE',
        Loop_Index.DRAWING.value  : 'TRUE',
        Loop_Index.CLASSNAME.value: 'ILP'
        }
    
    # Order of columns as per the SQL script
    column_order = [
        Loop_Index.AREA.value,
        Loop_Index.LOOP_NO.value,
        Loop_Index.FUNCTION.value,
        Loop_Index.NUMBER.value,
        Loop_Index.SUFFIX.value,
        Loop_Index.LOOP_SERVICE_1.value,
        Loop_Index.STATUS.value,
        Loop_Index.WIRED.value,
        Loop_Index.DRAWING.value,
        Loop_Index.REMARKS.value,
        Loop_Index.CLASSNAME.value,
        Loop_Index.FORMATNAME.value,
        Loop_Index.AREAPATH.value,
        Loop_Index.LOOP_FUNCTION.value,
        Loop_Index.LOOP_SERVICE_2.value,
        Loop_Index.LOOP_SERVICE_3.value,
        Loop_Index.SIFPRO_RELEVANT.value,
        Loop_Index.FUNC_DESCRIPTION.value,
        Loop_Index.RESP_WORK_CENTER.value,
        Loop_Index.AIR_DISTRIBUTOR.value,
        Loop_Index.VAR_PART_OF_DRAWING_NO.value,
        Loop_Index.CLASSIFICATION_BY.value,
        Loop_Index.SUPPL_CHAR_1.value,
        Loop_Index.VISUAL_INSPECTION.value,
        ]
    
    # Reducing the dataframes to necessary columns
    loop_index_dataframe = loop_index_dataframe[list(
        simple_column_mapping.keys()) + [S_LoopIndex.CLASS.value, S_LoopIndex.DATABASE_NAME.value]].copy()
    database_names_dataframe = database_names_dataframe[[DatabaseNames.DATABASE_NAME.value]].copy()
    
    # Add columns with constant values
    for column, value in constant_value_assignments.items():
        loop_index_dataframe.loc[:, column] = value
    
    # Renaming the columns as per the simple mappings
    loop_index_dataframe.rename(
        columns=simple_column_mapping,
        inplace=True)
    
    # Filter rows where CLASS is 'Instrumentation' and DATABASE_NAME is in database_names_dataframe
    filter_class = loop_index_dataframe[S_LoopIndex.CLASS.value] == 'Instrumentation'
    filter_database_name = loop_index_dataframe[S_LoopIndex.DATABASE_NAME.value].isin(
            database_names_dataframe[DatabaseNames.DATABASE_NAME.value])
    loop_index_dataframe = loop_index_dataframe[filter_class & filter_database_name]
    
    # Reordering the columns as per the SQL script
    loop_index_dataframe = loop_index_dataframe.reindex(
        columns=column_order)
    
    return loop_index_dataframe
