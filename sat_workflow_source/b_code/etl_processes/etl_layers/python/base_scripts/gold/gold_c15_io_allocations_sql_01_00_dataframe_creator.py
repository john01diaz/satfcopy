import pandas
from sat_workflow_source.b_code.etl_schemas.silver_stage.s_io_allocations import S_IO_Allocations


def filter_dataframe(
        s_io_allocations_dataframe: pandas.DataFrame,
        vw_database_names_dataframe: pandas.DataFrame) \
        -> pandas.DataFrame:
    valid_classes = ['Instrumentation', 'Inst(Shared)', 'Elec(Shared)']
    valid_databases = vw_database_names_dataframe['database_name'].unique()
    
    is_valid_class = s_io_allocations_dataframe[S_IO_Allocations.CLASS.value].isin(
        valid_classes)
    is_valid_database = s_io_allocations_dataframe[S_IO_Allocations.DATABASE_NAME.value].isin(
        valid_databases)
    
    return s_io_allocations_dataframe[is_valid_class & is_valid_database]


def select_distinct_columns(
        filtered_dataframe: pandas.DataFrame) -> pandas.DataFrame:
    columns_to_select = [
        S_IO_Allocations.TAG_NUMBER.value,
        S_IO_Allocations.PARENT_EQUIPMENT_NO.value,
        S_IO_Allocations.IOTYPE.value,
        S_IO_Allocations.EQUIPMENT_NO.value,
        S_IO_Allocations.CATALOGUENO.value,
        S_IO_Allocations.CHANNELNUMBER.value,
        ]
    
    return filtered_dataframe[columns_to_select].drop_duplicates()


def create_dataframe_gold_c15_io_allocations_sql_01_00(
    input_tables: dict) -> pandas.DataFrame:
    s_io_allocations_dataframe = \
        input_tables['S_IO_Allocations']
    
    database_names_dataframe = \
        input_tables['database_names']

    filtered_dataframe = \
        filter_dataframe(
            s_io_allocations_dataframe,
            database_names_dataframe)
    
    output_dataframe = \
        select_distinct_columns(
            filtered_dataframe)
    
    return \
        output_dataframe
