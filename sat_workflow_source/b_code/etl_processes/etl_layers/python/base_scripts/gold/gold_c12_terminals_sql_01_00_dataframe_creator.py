from sat_workflow_source.b_code.etl_processes.common.constants import DEFAULT_CELL_VALUE
from sat_workflow_source.b_code.etl_schemas.common.database_names import DatabaseNames
from sat_workflow_source.b_code.etl_schemas.gold_stage.terminals import Terminals
from sat_workflow_source.b_code.etl_schemas.silver_stage.s_terminals import S_Terminals


def dense_rank(
        df,
        partition_columns,
        order_by_column):
    df = df.sort_values(
            by=partition_columns + [order_by_column])
    df[Terminals.SEQUENCE.value] = df.groupby(
            partition_columns).rank(
            method='dense').astype(int)
    
    df[Terminals.SEQUENCE.value].round(0)
    return df

def create_dataframe_gold_c12_terminals_sql_01_00(
        input_tables: dict):
    terminals_dataframe = \
        input_tables['S_Terminals']
    
    database_names_dataframe = \
        input_tables['database_names']
    # Define the classes we're interested in
    classes_of_interest = ['Instrumentation', 'Inst(Shared)', 'Elec(Shared)']
    
    # Filter terminals by the classes_of_interest
    filtered_terminals = terminals_dataframe[terminals_dataframe[S_Terminals.CLASS.value].isin(
            classes_of_interest)]
    
    # Filter terminals where the database_name is in the database_names dataframe
    database_names_list = database_names_dataframe[DatabaseNames.DATABASE_NAME.value].unique()
    filtered_terminals = filtered_terminals[filtered_terminals[S_Terminals.DATABASE_NAME.value].isin(
            database_names_list)]
    
    # Select distinct rows
    filtered_terminals = filtered_terminals[[S_Terminals.PARENT_EQUIPMENT_NO.value,
                                             S_Terminals.EQUIPMENT_NO.value,
                                             S_Terminals.MARKING.value]].drop_duplicates()
    
    # Apply the dense_rank function
    final_terminals = dense_rank(
            df=filtered_terminals,
            partition_columns=[S_Terminals.PARENT_EQUIPMENT_NO.value, S_Terminals.EQUIPMENT_NO.value],
            order_by_column=S_Terminals.MARKING.value)

    final_terminals[Terminals.EQUIPMENT_NO.value] = DEFAULT_CELL_VALUE
    
    return final_terminals
