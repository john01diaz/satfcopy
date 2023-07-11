from sat_workflow_source.b_code.etl_processes.common.constants import DEFAULT_CELL_VALUE
from sat_workflow_source.b_code.etl_schemas.common.database_names import DatabaseNames
from sat_workflow_source.b_code.etl_schemas.gold_stage.io_catalogue import IO_Catalogue
from sat_workflow_source.b_code.etl_schemas.silver_stage.s_io_catalogue import S_IO_Catalogue


def __filter_by_database_names(dataframe, database_names):
    return dataframe[
        dataframe[S_IO_Catalogue.DATABASE_NAME.value].isin(database_names[DatabaseNames.DATABASE_NAME.value])]


def __filter_by_catalogue_rnt(dataframe):
    return dataframe[dataframe[S_IO_Catalogue.CATALOGUE_RNT.value] == "1"]


def __sort_dataframe(dataframe):
    dataframe[f"{S_IO_Catalogue.CHANNEL.value}_transformed"] = dataframe[S_IO_Catalogue.CHANNEL.value].str.replace(
        r'AI_,AO_,DI_,DO_,FIELD BUS_', "0")
    dataframe[f"{S_IO_Catalogue.CHANNEL.value}_transformed"] = dataframe[
        f"{S_IO_Catalogue.CHANNEL.value}_transformed"].apply(lambda x: int(x) if x != '' else 0)
    dataframe = dataframe.sort_values(by=[S_IO_Catalogue.MODEL_NUMBER.value,
                                          S_IO_Catalogue.IOTYPE.value,
                                          f"{S_IO_Catalogue.CHANNEL.value}_transformed",
                                          S_IO_Catalogue.TERMINALSPERMARKING.value])
    return dataframe


def __select_columns(dataframe):
    return dataframe[[S_IO_Catalogue.MODEL_NUMBER.value,
                      S_IO_Catalogue.DESCRIPTION.value,
                      S_IO_Catalogue.MANUFACTURER.value,
                      S_IO_Catalogue.DESCRIPTIONDRAWING.value,
                      S_IO_Catalogue.CHANNEL.value,
                      S_IO_Catalogue.ALLOWUSE.value,
                      S_IO_Catalogue.IOTYPE.value,
                      S_IO_Catalogue.NOOFPOINTS.value,
                      S_IO_Catalogue.TERMINALSPERPOINTCHANNEL.value,
                      S_IO_Catalogue.TERMINALSPERMARKING.value,
                      S_IO_Catalogue.CHANNELNUMBER.value]]


def create_dataframe_gold_c06_io_catalogue_sql_01_00(input_tables):
    io_catalogue_dataframe = \
        input_tables['Sigraph_Silver.S_IO_Catalogue']

    vw_database_names_dataframe = \
        input_tables['VW_Database_names']

    filtered_dataframe = __filter_by_database_names(io_catalogue_dataframe, vw_database_names_dataframe)
    filtered_dataframe = __filter_by_catalogue_rnt(filtered_dataframe)

    filtered_dataframe = __sort_dataframe(filtered_dataframe)

    result_dataframe = __select_columns(filtered_dataframe)

    return result_dataframe