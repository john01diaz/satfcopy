import pandas
import pandas as pd

from sat_workflow_source.b_code.etl_schemas.common.database_names import DatabaseNames
from sat_workflow_source.b_code.etl_schemas.silver_stage.s_io_catalogue import S_IO_Catalogue

def _filter_dataframe_before_merge(dataframe, columns_to_keep):
    dataframe_filtered = dataframe[columns_to_keep]
    return dataframe_filtered
def _create_catalogue_raw(dataframe_io_card_prep_query, dataframe_io_terminal_marking):
    columns_to_keep_io_card_prep_query = [
        DatabaseNames.DATABASE_NAME.value,
        S_IO_Catalogue.DYNAMIC_CLASS.value,
        S_IO_Catalogue.OBJECT_IDENTIFIER.value,
        S_IO_Catalogue.MODEL_NUMBER.value,
        S_IO_Catalogue.DESCRIPTION.value,
        S_IO_Catalogue.MANUFACTURER.value,
        S_IO_Catalogue.IOTYPE.value,
        S_IO_Catalogue.NOOFPOINTS.value,
        S_IO_Catalogue.CLASS.value
    ]
    columns_to_keep_io_terminal_marking = [
        DatabaseNames.DATABASE_NAME.value,
        S_IO_Catalogue.DYNAMIC_CLASS.value,
        S_IO_Catalogue.OBJECT_IDENTIFIER.value,
        S_IO_Catalogue.TERMINALSPERMARKING.value
    ]
    dataframe_io_card_prep_query_filtered = _filter_dataframe_before_merge(dataframe_io_card_prep_query, columns_to_keep_io_card_prep_query)
    dataframe_io_terminal_marking_filtered = _filter_dataframe_before_merge(dataframe_io_terminal_marking, columns_to_keep_io_terminal_marking)

    # Joining the filtered dataframes
    dataframe_io_catalogue_raw = pd.merge(
        dataframe_io_card_prep_query_filtered,
        dataframe_io_terminal_marking_filtered,
        how='inner',
        on=[DatabaseNames.DATABASE_NAME.value, S_IO_Catalogue.DYNAMIC_CLASS.value, S_IO_Catalogue.OBJECT_IDENTIFIER.value]
    )

    # Creating a new column 'Catalogue_RNT' as the row number over the partition of 'Model_Number', 'NoOfPoints', 'TerminalsPerMarking'
    dataframe_io_catalogue_raw[S_IO_Catalogue.CATALOGUE_RNT.value] = dataframe_io_catalogue_raw.sort_values(
        [S_IO_Catalogue.MODEL_NUMBER.value, S_IO_Catalogue.NOOFPOINTS.value, S_IO_Catalogue.TERMINALSPERMARKING.value]
    ).groupby([S_IO_Catalogue.MODEL_NUMBER.value, S_IO_Catalogue.NOOFPOINTS.value, S_IO_Catalogue.TERMINALSPERMARKING.value]).cumcount() + 1

    return dataframe_io_catalogue_raw

def _create_io_catalogue(dataframe_io_catalogue_raw):
    dataframe_io_catalogue = dataframe_io_catalogue_raw.copy()

    # Adding new columns with constant values
    dataframe_io_catalogue[S_IO_Catalogue.DESCRIPTIONDRAWING.value] = ''
    dataframe_io_catalogue[S_IO_Catalogue.CHANNEL.value] = ''
    dataframe_io_catalogue[S_IO_Catalogue.ALLOWUSE.value] = 'True'
    dataframe_io_catalogue[S_IO_Catalogue.TERMINALSPERPOINTCHANNEL.value] = 2

    # Ordering by 'Model_Number', 'NoOfPoints', 'ChannelNumber'
    dataframe_io_catalogue = dataframe_io_catalogue.sort_values(
        [S_IO_Catalogue.MODEL_NUMBER.value, S_IO_Catalogue.NOOFPOINTS.value, S_IO_Catalogue.CHANNEL.value]
    )

    return dataframe_io_catalogue


def silver_c10_s_IO_Catalogue_sql_03_00_sql_00_00_dataframe_creator(
        input_tables: dict) -> pandas.DataFrame:
    
    dataframe_io_terminal_marking = \
        input_tables['VW_IO_TerminalMarking']
    
    dataframe_io_card_prep_query = \
        input_tables['VW_IOCard_Prep_Query']
    
    dataframe_io_catalogue_raw = _create_catalogue_raw(dataframe_io_card_prep_query, dataframe_io_terminal_marking)
    dataframe_io_catalogue = _create_io_catalogue(dataframe_io_catalogue_raw)

    return dataframe_io_catalogue
