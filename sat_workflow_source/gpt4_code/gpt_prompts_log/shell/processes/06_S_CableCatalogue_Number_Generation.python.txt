def __get_dataframes(input_tables: dict) -> tuple:
    s_cablecorecatalogue_dataframe = input_tables['Sigraph_Silver.S_CableCoreCatalogue']
    s_cablecatalogue_dataframe = input_tables['Sigraph_Silver.S_CableCatalogue']
    return s_cablecorecatalogue_dataframe, s_cablecatalogue_dataframe

def __join_dataframes(s_cablecorecatalogue_dataframe: DataFrame, s_cablecatalogue_dataframe: DataFrame) -> DataFrame:
    joined_dataframe = pandas.merge(
        s_cablecorecatalogue_dataframe,
        s_cablecatalogue_dataframe,
        left_on=[S_CableCoreCatalogue.DATABASE_NAME.value,
                 S_CableCoreCatalogue.CABLE_OBJECT_IDENTIFIER.value],
        right_on=[S_CableCatalogue.DATABASE_NAME.value,
                  S_CableCatalogue.OBJECT_IDENTIFIER.value])
    joined_dataframe = joined_dataframe.loc[joined_dataframe[S_CableCoreCatalogue.CABLE_OBJECT_IDENTIFIER.value].notnull()]
    return joined_dataframe

def __clean_description(joined_dataframe: DataFrame) -> DataFrame:
    joined_dataframe[S_CableCatalogue.DESCRIPTION.value] = joined_dataframe[
        S_CableCatalogue.DESCRIPTION.value].str.replace(
        '% GRAU MMA',
        '').str.replace(
        ' BLAU MMA',
        '').str.replace(
        ' BLAU',
        '').str.replace(
        ' GRAU',
        '').str.replace(
        ' ROT',
        '').str.strip()
    joined_dataframe = joined_dataframe.drop_duplicates()
    return joined_dataframe

def __create_markings(joined_dataframe: DataFrame) -> DataFrame:
    joined_dataframe['Markings'] = joined_dataframe.groupby(
            [S_CableCoreCatalogue.DATABASE_NAME.value,
             S_CableCoreCatalogue.CABLE_OBJECT_IDENTIFIER.value])[
        S_CableCoreCatalogue.CORE_MARKINGS.value].transform(
        lambda x: ','.join(sorted(set(x))))
    joined_dataframe = joined_dataframe.drop_duplicates()
    return joined_dataframe

def __sort_and_rank(joined_dataframe: DataFrame) -> DataFrame:
    joined_dataframe = joined_dataframe.sort_values(by=[S_CableCatalogue.DESCRIPTION.value, 'Markings'])
    joined_dataframe['Rank'] = joined_dataframe.groupby([S_CableCatalogue.DESCRIPTION.value, 'Markings']).cumcount() + 1
    return joined_dataframe

def __prepare_result(joined_dataframe: DataFrame) -> DataFrame:
    joined_dataframe[S_CableCatalogueNumber_Master.CATALOGUENO.value] = 'SHRH_CABLE_' + (joined_dataframe['Rank'] + 2000).astype(str)
    result_dataframe = joined_dataframe[[S_CableCatalogueNumber_Master.CATALOGUENO.value,
                                         S_CableCatalogue.DESCRIPTION.value,
                                         S_CableCoreCatalogue.DATABASE_NAME.value,
                                         S_CableCoreCatalogue.CABLE_OBJECT_IDENTIFIER.value]]
    result_dataframe[S_CableCatalogueNumber_Master.CATALOGUENO.value] = DEFAULT_CELL_VALUE
    return result_dataframe

def create_silver_06_s_cablecatalogue_number_generation_dataframe(input_tables: dict) -> DataFrame:
    s_cablecorecatalogue_dataframe, s_cablecatalogue_dataframe = __get_dataframes(input_tables)
    joined_dataframe = __join_dataframes(s_cablecorecatalogue_dataframe, s_cablecatalogue_dataframe)
    joined_dataframe = __clean_description(joined_dataframe)
    joined_dataframe = __create_markings(joined_dataframe)
    joined_dataframe = __sort_and_rank(joined_dataframe)
    result_dataframe = __prepare_result(joined_dataframe)
    return result_dataframe