from enum import Enum

import pandas

from sat_workflow_source.b_code.etl_schemas.gold_stage.cable_core_catalogue import Cable_Core_Catalogue
from sat_workflow_source.b_code.etl_schemas.silver_stage.s_cable_catalogue import S_CableCatalogue
from sat_workflow_source.b_code.etl_schemas.silver_stage.s_cable_catalogue_number_master import \
    S_CableCatalogueNumber_Master
from sat_workflow_source.b_code.etl_schemas.silver_stage.s_cable_core_catalogue import S_CableCoreCatalogue


# Make sure to import the enums at the start of your script


def create_dataframe_gold_c03_cable_core_catalogue_demagicked(
        input_tables: dict) -> pandas.DataFrame:
    
    cable_core_catalogue_df = \
        input_tables['S_CableCoreCatalogue']
    
    cable_catalogue_df = \
        input_tables['S_CableCatalogue']
    
    cable_catalogue_number_master_df = \
        input_tables['S_CableCatalogueNumber_Master']
    
    database_names_df = \
        input_tables['database_names']
    
    # Merge the dataframes
    merged_df = cable_core_catalogue_df.merge(
        cable_catalogue_number_master_df,
        left_on=[S_CableCoreCatalogue.DATABASE_NAME.value, S_CableCoreCatalogue.CABLE_OBJECT_IDENTIFIER.value],
        right_on=[S_CableCatalogueNumber_Master.DATABASE_NAME.value,
                  S_CableCatalogueNumber_Master.CABLE_OBJECT_IDENTIFIER.value])
    merged_df = merged_df.merge(
        cable_catalogue_df,
        left_on=[S_CableCatalogue.DATABASE_NAME.value, S_CableCatalogue.OBJECT_IDENTIFIER.value],
        right_on=[S_CableCatalogue.DATABASE_NAME.value, S_CableCatalogue.OBJECT_IDENTIFIER.value])
    
    # Filter based on the class and database_name
    filtered_df = merged_df[merged_df[S_CableCatalogue.CLASS.value].isin(
            ['Instrumentation', 'Inst(Shared)', 'Elec(Shared)'])]
    filtered_df = filtered_df[filtered_df[S_CableCoreCatalogue.DATABASE_NAME.value].isin(
            database_names_df['Database_name'])]
    
    # Add a column for dense_rank equivalent
    filtered_df['combined_column'] = filtered_df[S_CableCatalogueNumber_Master.DATABASE_NAME.value] + '_' + filtered_df[
        S_CableCatalogueNumber_Master.CABLE_OBJECT_IDENTIFIER.value]
    filtered_df['RNT'] = filtered_df.groupby(
        S_CableCatalogueNumber_Master.CATALOGUENO.value)['combined_column'].rank(
        method='dense')

    final_df = __create_vw_cable_data_rnt_1(filtered_df)
    
    return final_df


def __create_vw_cable_data_rnt_1(
        cable_data_df):
    # Filter the dataframe to keep only the rows where RNT is 1
    cable_data_rnt_1_df = cable_data_df[cable_data_df['RNT'] == 1]
    
    # Keep only the required columns
    cable_data_rnt_1_df = cable_data_rnt_1_df[[Cable_Core_Catalogue.CATALOGUENO.value,
                                               Cable_Core_Catalogue.DESCRIPTION.value,
                                               Cable_Core_Catalogue.GROUP_MARKING.value,
                                               Cable_Core_Catalogue.GROUP_MARKING_SEQUENCE.value,
                                               Cable_Core_Catalogue.CORE_MARKINGS.value,
                                               Cable_Core_Catalogue.CORE_MARKINGS_CORE_TYPE.value]]
    return cable_data_rnt_1_df
