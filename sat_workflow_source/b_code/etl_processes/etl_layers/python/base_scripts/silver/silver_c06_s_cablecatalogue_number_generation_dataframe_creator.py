import pandas
from pandas import DataFrame

from sat_workflow_source.b_code.etl_processes.common.constants import DEFAULT_CELL_VALUE
from sat_workflow_source.b_code.etl_schemas.silver_stage.s_cable_catalogue import S_CableCatalogue
from sat_workflow_source.b_code.etl_schemas.silver_stage.s_cable_catalogue_number_master import \
    S_CableCatalogueNumber_Master
from sat_workflow_source.b_code.etl_schemas.silver_stage.s_cable_core_catalogue import S_CableCoreCatalogue


def create_silver_06_s_cablecatalogue_number_generation_dataframe(
        input_tables: dict) \
        -> DataFrame:
    s_cablecorecatalogue_dataframe = \
        input_tables['S_CableCoreCatalogue']

    s_cablecatalogue_dataframe = \
        input_tables['S_CableCatalogue']

    joined_dataframe = pandas.merge(
        s_cablecorecatalogue_dataframe,
        s_cablecatalogue_dataframe,
        left_on=[S_CableCoreCatalogue.DATABASE_NAME.value,
                 S_CableCoreCatalogue.CABLE_OBJECT_IDENTIFIER.value],
        right_on=[S_CableCatalogue.DATABASE_NAME.value,
                  S_CableCatalogue.OBJECT_IDENTIFIER.value])
    
    joined_dataframe = joined_dataframe.loc[
        joined_dataframe[S_CableCoreCatalogue.CABLE_OBJECT_IDENTIFIER.value].notnull()]
    
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
    
    joined_dataframe['Markings'] = joined_dataframe.groupby(
            [S_CableCoreCatalogue.DATABASE_NAME.value,
             S_CableCoreCatalogue.CABLE_OBJECT_IDENTIFIER.value])[
        S_CableCoreCatalogue.CORE_MARKINGS.value].transform(
        lambda
            x: ','.join(
            sorted(
                set(
                    x))))
    
    joined_dataframe = joined_dataframe.drop_duplicates()
    
    joined_dataframe = joined_dataframe.sort_values(
        by=[S_CableCatalogue.DESCRIPTION.value, 'Markings'])
    
    joined_dataframe['Rank'] = joined_dataframe.groupby(
            [S_CableCatalogue.DESCRIPTION.value, 'Markings']).cumcount() + 1
    
    joined_dataframe[S_CableCatalogueNumber_Master.CATALOGUENO.value] = 'SHRH_CABLE_' + (
                joined_dataframe['Rank'] + 2000).astype(
        str)
    
    result_dataframe = joined_dataframe[[S_CableCatalogueNumber_Master.CATALOGUENO.value,
                                         S_CableCatalogue.DESCRIPTION.value,
                                         S_CableCoreCatalogue.DATABASE_NAME.value,
                                         S_CableCoreCatalogue.CABLE_OBJECT_IDENTIFIER.value]]
    
    # Added line for column exclusion
    
    result_dataframe[S_CableCatalogueNumber_Master.CATALOGUENO.value] = \
        DEFAULT_CELL_VALUE
    
    
    return result_dataframe