from sat_workflow_source.b_code.etl_processes.common.constants import DEFAULT_CELL_VALUE
from sat_workflow_source.b_code.etl_schemas.common.database_names import DatabaseNames
from sat_workflow_source.b_code.etl_schemas.gold_stage.terminations import Terminations
from sat_workflow_source.b_code.etl_schemas.silver_stage.s_terminations import S_Terminations


def create_dataframe_gold_c13_terminations_sql_01_00(
        input_tables: dict):
    terminations_dataframe = \
        input_tables['S_Terminations']
    
    database_names_dataframe = \
        input_tables['database_names']

    # Define the classes we're interested in
    classes_of_interest = ['Instrumentation', 'Inst(Shared)', 'Elec(Shared)']
    
    # Filter terminations by the classes_of_interest
    filtered_terminations = terminations_dataframe[terminations_dataframe[S_Terminations.CLASS.value].isin(
            classes_of_interest)]
    
    # Filter terminations where the database_name is in the database_names dataframe
    database_names_list = database_names_dataframe[DatabaseNames.DATABASE_NAME.value].unique()
    filtered_terminations = filtered_terminations[filtered_terminations[S_Terminations.DATABASE_NAME.value].isin(
            database_names_list)]
    
    # Select distinct rows
    filtered_terminations = filtered_terminations[[S_Terminations.CABLENUMBER.value,
                                                   S_Terminations.CORE_MARKINGS.value,
                                                   S_Terminations.PARENT_EQUIPMENT_NO.value,
                                                   S_Terminations.EQUIPMENT_NO.value,
                                                   S_Terminations.MARKING.value,
                                                   S_Terminations.LEFT_RIGHT.value]].drop_duplicates()
    filtered_terminations.replace(
            {
                None:'null'
                },
            inplace=True)

    filtered_terminations[Terminations.EQUIPMENT_NO.value] = DEFAULT_CELL_VALUE
    
    return filtered_terminations
