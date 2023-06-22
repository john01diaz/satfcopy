def load_data(
        s_io_catalogue_parquet_path,
        vw_database_names_parquet_path):
    s_io_catalogue_dataframe = pd.read_parquet(
            s_io_catalogue_parquet_path)
    vw_database_names_dataframe = pd.read_parquet(
            vw_database_names_parquet_path)
    return s_io_catalogue_dataframe, vw_database_names_dataframe


def filter_data(
        s_io_catalogue_dataframe,
        vw_database_names_dataframe):
    filtered_dataframe = s_io_catalogue_dataframe[s_io_catalogue_dataframe[S_IO_Catalogue.CATALOGUE_RNT.value] == 1]
    filtered_dataframe = filtered_dataframe[filtered_dataframe[S_IO_Catalogue.DATABASE_NAME.value].isin(
            vw_database_names_dataframe[DatabaseNames.DATABASE_NAME.value])]
    return filtered_dataframe


def transform_and_sort_data(
        filtered_dataframe):
    filtered_dataframe[S_IO_Catalogue.TERMINALSPERMARKING.value] = filtered_dataframe[
        S_IO_Catalogue.TERMINALSPERMARKING.value].str.replace(
            '+',
            '').str.replace(
            '-',
            '').astype(
            int)
    sorted_dataframe = filtered_dataframe.sort_values(
            by=[S_IO_Catalogue.MODEL_NUMBER.value, S_IO_Catalogue.TERMINALSPERMARKING.value])
    return sorted_dataframe


def main():
    s_io_catalogue_parquet_path = 'path_to_s_io_catalogue.parquet'
    vw_database_names_parquet_path = 'path_to_vw_database_names.parquet'
    
    s_io_catalogue_dataframe, vw_database_names_dataframe = load_data(
            s_io_catalogue_parquet_path,
            vw_database_names_parquet_path)
    filtered_dataframe = filter_data(
            s_io_catalogue_dataframe,
            vw_database_names_dataframe)
    sorted_dataframe = transform_and_sort_data(
            filtered_dataframe)
    
    io_catalogue_columns = [S_IO_Catalogue.MODEL_NUMBER.value, S_IO_Catalogue.DESCRIPTION.value,
                            S_IO_Catalogue.MANUFACTURER.value,
                            S_IO_Catalogue.DESCRIPTIONDRAWING.value, S_IO_Catalogue.CHANNEL.value,
                            S_IO_Catalogue.ALLOWUSE.value,
                            S_IO_Catalogue.IOTYPE.value, S_IO_Catalogue.NOOFPOINTS.value,
                            S_IO_Catalogue.TERMINALSPERPOINTCHANNEL.value,
                            S_IO_Catalogue.TERMINALSPERMARKING.value]
    
    distinct_dataframe = sorted_dataframe[io_catalogue_columns].drop_duplicates()
    
    return distinct_dataframe
