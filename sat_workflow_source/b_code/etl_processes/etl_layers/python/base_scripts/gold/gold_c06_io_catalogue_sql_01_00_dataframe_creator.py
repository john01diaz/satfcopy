from sat_workflow_source.b_code.etl_processes.common.constants import DEFAULT_CELL_VALUE
from sat_workflow_source.b_code.etl_schemas.common.database_names import DatabaseNames
from sat_workflow_source.b_code.etl_schemas.gold_stage.io_catalogue import IO_Catalogue
from sat_workflow_source.b_code.etl_schemas.silver_stage.s_io_catalogue import S_IO_Catalogue


def create_dataframe_gold_c06_io_catalogue_sql_01_00(
        input_tables: dict):
    io_catalogue_dataframe = \
        input_tables['Sigraph_Silver.S_IO_Catalogue']
    
    vw_database_names_dataframe = \
        input_tables['VW_Database_names']

    """
    Function to execute the SQL query equivalent in Python

    Parameters:
    s_io_catalogue_dataframe (pandas.DataFrame): Dataframe equivalent to SIGRAPH_SILVER.S_IO_CATALOGUE
    database_names_dataframe (pandas.DataFrame): Dataframe equivalent to VW_Database_names

    Returns:
    pandas.DataFrame: Result of the query execution
    """
    CATALOGUE_RNT_VALUE = 1
    # Filtering data where Catalogue_RNT = 1
    catalogue_rnt_filtered_dataframe = io_catalogue_dataframe[
        io_catalogue_dataframe[S_IO_Catalogue.CATALOGUE_RNT.value] == CATALOGUE_RNT_VALUE]
    
    # Further filtering where database_name is in database_names_dataframe
    database_names_filtered_dataframe = catalogue_rnt_filtered_dataframe[
        catalogue_rnt_filtered_dataframe[S_IO_Catalogue.DATABASE_NAME.value].isin(
                vw_database_names_dataframe[DatabaseNames.DATABASE_NAME.value])]
    
    # database_names_filtered_dataframe[S_IO_Catalogue.TERMINALSPERMARKING.value] = (
    #         database_names_filtered_dataframe[S_IO_Catalogue.TERMINALSPERMARKING.value]
    #         .replace(
    #             '\+|-',
    #             '',
    #             regex=True)
    #     )
    #
    # pandas.to_numeric(
    #         database_names_filtered_dataframe[S_IO_Catalogue.TERMINALSPERMARKING.value],
    #         errors='coerce')
    
    # Sorting by Model_Number and TerminalsPerMarking
    sorted_dataframe = database_names_filtered_dataframe.sort_values(
            by=[S_IO_Catalogue.MODEL_NUMBER.value, S_IO_Catalogue.TERMINALSPERMARKING.value])
    
    # Selecting distinct rows
    distinct_dataframe = sorted_dataframe.drop_duplicates()
    
    output_dataframe = distinct_dataframe[[IO_Catalogue.MODEL_NUMBER.value,
                                           IO_Catalogue.DESCRIPTION.value,
                                           IO_Catalogue.MANUFACTURER.value,
                                           IO_Catalogue.DESCRIPTIONDRAWING.value,
                                           IO_Catalogue.CHANNEL.value,
                                           IO_Catalogue.ALLOWUSE.value,
                                           IO_Catalogue.IOTYPE.value,
                                           IO_Catalogue.NOOFPOINTS.value,
                                           IO_Catalogue.TERMINALSPERPOINTCHANNEL.value,
                                           IO_Catalogue.TERMINALSPERMARKING.value]]

    output_dataframe[IO_Catalogue.TERMINALSPERMARKING.value] = \
        DEFAULT_CELL_VALUE

    return output_dataframe
