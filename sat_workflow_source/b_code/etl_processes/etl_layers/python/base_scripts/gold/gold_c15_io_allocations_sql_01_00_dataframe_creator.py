from sat_workflow_source.b_code.etl_schemas.common.database_names import DatabaseNames
from sat_workflow_source.b_code.etl_schemas.silver_stage.s_io_allocations import S_IO_Allocations


def get_database_names(
        database_names_dataframe):
    return database_names_dataframe[DatabaseNames.DATABASE_NAME.value].unique().tolist()


def filter_io_allocations_by_class(
        io_allocations_dataframe):
    class_values = ['Instrumentation', 'Inst(Shared)', 'Elec(Shared)']
    return io_allocations_dataframe[io_allocations_dataframe[S_IO_Allocations.CLASS.value].isin(
        class_values)]


def filter_io_allocations_by_database(
        io_allocations_dataframe,
        database_names):
    return io_allocations_dataframe[io_allocations_dataframe[S_IO_Allocations.DATABASE_NAME.value].isin(
        database_names)]


def select_columns(
        io_allocations_dataframe):
    columns_to_select = [
        S_IO_Allocations.PARENT_EQUIPMENT_NO.value,
        S_IO_Allocations.EQUIPMENT_NO.value,
        S_IO_Allocations.CATALOGUENO.value,
        S_IO_Allocations.TAG_NUMBER.value,
        S_IO_Allocations.IOTYPE.value,
        S_IO_Allocations.CHANNELNUMBER.value
        ]
    return io_allocations_dataframe[columns_to_select]


def get_distinct_io_allocations(
        io_allocations_dataframe):
    return io_allocations_dataframe.drop_duplicates()


def create_dataframe_gold_c15_io_allocations_sql_01_00(
        input_tables):
    io_allocations_dataframe = input_tables['Sigraph_Silver.S_IO_Allocations']
    
    database_names_dataframe = input_tables['VW_Database_names']
    
    database_names = get_database_names(
        database_names_dataframe)
    filtered_io_allocations_by_class = filter_io_allocations_by_class(
        io_allocations_dataframe)
    filtered_io_allocations_by_database = filter_io_allocations_by_database(
            filtered_io_allocations_by_class,
            database_names
            )
    selected_io_allocations = select_columns(
        filtered_io_allocations_by_database)
    return get_distinct_io_allocations(
        selected_io_allocations)
