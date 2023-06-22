

import pandas as pd
from enum import Enum

# Enum classes defined here... (see above)
from sat_workflow_source.b_code.etl_schemas.silver_stage.s_cable_catalogue import S_CableCatalogueColumns
from sat_workflow_source.b_code.etl_schemas.silver_stage.s_cable_core_catalogue_columns import S_CableCoreCatalogueColumns


def create_silver_06_s_cablecatalogue_number_generation_dataframe(
        s_cablecorecatalogue_dataframe,
        s_cablecatalogue_dataframe):
        
    # Constants
    SHRH_CABLE = 'SHRH_CABLE_'
    BASE_RANK = 2000
    REPLACEMENTS = [
        ('% GRAU MMA', ''),
        (' BLAU MMA', ''),
        (' BLAU', ''),
        (' GRAU', ''),
        (' ROT', '')
        ]
    
    # Assume that we have dataframes s_cablecorecatalogue_dataframe and s_cablecatalogue_dataframe
    # Remove certain substrings from the original Description column
    for old, new in REPLACEMENTS:
        s_cablecatalogue_dataframe[S_CableCatalogueColumns.description.value] = s_cablecatalogue_dataframe[
            S_CableCatalogueColumns.description.value].str.replace(
            old,
            new)
    s_cablecatalogue_dataframe[S_CableCatalogueColumns.description.value] = s_cablecatalogue_dataframe[
        S_CableCatalogueColumns.description.value].str.strip()
    
    # Merge the two dataframes
    merged_df = pd.merge(
        s_cablecorecatalogue_dataframe,
        s_cablecatalogue_dataframe,
        left_on=[S_CableCoreCatalogueColumns.database_name.value,
                 S_CableCoreCatalogueColumns.cable_object_identifier.value],
        right_on=[S_CableCatalogueColumns.database_name.value, S_CableCatalogueColumns.object_identifier.value]
        )
    
    # Remove rows where Cable_Object_Identifier is null
    merged_df = merged_df[merged_df[S_CableCoreCatalogueColumns.cable_object_identifier.value].notna()]
    
    # Generate Markings by concatenating Core_Markings of all records that share the same database_name and
    # Cable_Object_Identifier
    merged_df['Markings'] = merged_df.groupby(
        [S_CableCoreCatalogueColumns.database_name.value, S_CableCoreCatalogueColumns.cable_object_identifier.value])[
        S_CableCoreCatalogueColumns.core_markings.value].transform(
        lambda
            x: ','.join(
            sorted(
                x)))
    
    # Drop duplicate rows
    merged_df = merged_df.drop_duplicates()
    
    # Generate a dense rank for each unique combination of Description and Markings
    merged_df['Rank'] = merged_df.groupby(
        [S_CableCatalogueColumns.description.value, 'Markings']).ngroup()
    
    # Create CatalogueNo
    merged_df['CatalogueNo'] = SHRH_CABLE + (BASE_RANK + merged_df['Rank']).astype(
        str)
    
    # Create the final view VW_CatalogueNo by selecting the relevant columns
    silver_06_s_cablecatalogue_number_generation_dataframe = merged_df[
        ['CatalogueNo', S_CableCatalogueColumns.description.value, S_CableCoreCatalogueColumns.database_name.value,
         S_CableCoreCatalogueColumns.cable_object_identifier.value]]
    
    # Equivalent of 'Create Or Replace Temp View VW_CatalogueNo' in SQL
    silver_06_s_cablecatalogue_number_generation_dataframe.to_csv(
        'VW_CatalogueNo.csv',
        index=False)
    
    return \
        silver_06_s_cablecatalogue_number_generation_dataframe
