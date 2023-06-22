import pandas
from pandas import DataFrame
from pandas import merge

from sat_workflow_source.b_code.etl_schemas.silver_stage.s_cable_catalogue import S_CableCatalogueColumns
from sat_workflow_source.b_code.etl_schemas.silver_stage.s_cable_catalogue_number_master_columns import \
    S_CableCatalogueNumberMasterColumns
from sat_workflow_source.b_code.etl_schemas.silver_stage.s_cable_core_catalogue_columns import \
    S_CableCoreCatalogueColumns


def create_silver_06_s_cablecatalogue_number_generation_dataframe(
        s_cablecorecatalogue_dataframe: DataFrame,
        s_cablecatalogue_dataframe: DataFrame):
    
    REPLACE_GRAU_MMA = '% GRAU MMA'
    REPLACE_BLAU_MMA = ' BLAU MMA'
    REPLACE_BLAU = ' BLAU'
    REPLACE_GRAU = ' GRAU'
    REPLACE_ROT = ' ROT'
    CATALOGUE_NO_PREFIX = 'SHRH_CABLE_'
    MARKINGS = 'Markings'
    RANK = 'rank'
    
    merged_dataframe = merge(
        s_cablecorecatalogue_dataframe,
        s_cablecatalogue_dataframe,
        left_on=[S_CableCoreCatalogueColumns.DATABASE_NAME.value,
                 S_CableCoreCatalogueColumns.CABLE_OBJECT_IDENTIFIER.value],
        right_on=[S_CableCatalogueColumns.DATABASE_NAME.value,
                  S_CableCatalogueColumns.OBJECT_IDENTIFIER.value])
    
    merged_dataframe = merged_dataframe[
        merged_dataframe[S_CableCoreCatalogueColumns.CABLE_OBJECT_IDENTIFIER.value].notna()]
    
    merged_dataframe[S_CableCatalogueColumns.DESCRIPTION.value] = \
        merged_dataframe[
            S_CableCatalogueColumns.DESCRIPTION.value].apply(
            lambda
                desc: desc.replace(
                REPLACE_GRAU_MMA,
                '').replace(
                REPLACE_BLAU_MMA,
                '')
                .replace(
                REPLACE_BLAU,
                '').replace(
                REPLACE_GRAU,
                '').replace(
                REPLACE_ROT,
                '').strip()
            )
    
    merged_dataframe[MARKINGS] = \
        merged_dataframe.groupby(
            [S_CableCoreCatalogueColumns.DATABASE_NAME.value,
             S_CableCoreCatalogueColumns.CABLE_OBJECT_IDENTIFIER.value])[
            S_CableCoreCatalogueColumns.CORE_MARKINGS.value].transform(
            lambda
                x: ', '.join(
                sorted(
                    set(
                        x))))
    
    merged_dataframe = \
        merged_dataframe.drop_duplicates(
            subset=[S_CableCoreCatalogueColumns.DATABASE_NAME.value,
                    S_CableCoreCatalogueColumns.CABLE_OBJECT_IDENTIFIER.value,
                    S_CableCatalogueColumns.DESCRIPTION.value, MARKINGS])
    
    catalogue_master_dataframe = \
        merged_dataframe[[S_CableCoreCatalogueColumns.DATABASE_NAME.value,
                          S_CableCoreCatalogueColumns.CABLE_OBJECT_IDENTIFIER.value,
                          S_CableCatalogueColumns.DESCRIPTION.value, MARKINGS]]
    
    catalogue_master_dataframe = \
        catalogue_master_dataframe.sort_values(
            by=[S_CableCatalogueColumns.DESCRIPTION.value, MARKINGS])
    catalogue_master_dataframe[RANK] = \
        catalogue_master_dataframe.groupby(
            [S_CableCoreCatalogueColumns.DATABASE_NAME.value]).cumcount() + 2000
    catalogue_master_dataframe[S_CableCatalogueNumberMasterColumns.CATALOGUE_NO.value] = \
        CATALOGUE_NO_PREFIX + \
        catalogue_master_dataframe[
            RANK].astype(
            str)
    
    catalogue_master_dataframe = \
        catalogue_master_dataframe.drop(
            columns=[RANK, MARKINGS])
    
    return \
        catalogue_master_dataframe

# Please note that in pandas we don't have a direct equivalent to DENSE_RANK() in SQL.
# The closest one is to use groupby().cumcount() which gives us the cumulative count of the group, but it will not
# skip ranks if there are duplicate values as DENSE_RANK() does. In most cases this difference won't matter,
# but you should be aware of it if the precise ranking is important in your application.