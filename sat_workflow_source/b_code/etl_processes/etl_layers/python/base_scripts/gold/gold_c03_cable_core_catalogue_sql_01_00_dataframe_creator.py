from sat_workflow_source.b_code.etl_schemas.common.database_names import DatabaseNames
from sat_workflow_source.b_code.etl_schemas.silver_stage.s_cable_catalogue import S_CableCatalogue
from sat_workflow_source.b_code.etl_schemas.silver_stage.s_cable_catalogue_number_master import \
    S_CableCatalogueNumber_Master
from sat_workflow_source.b_code.etl_schemas.silver_stage.s_cable_core_catalogue import S_CableCoreCatalogue


def _get_database_names(
        input_tables):
    return input_tables[DatabaseNames.DATABASE_NAME.value]


def _get_cable_core_catalogue(
        input_tables):
    cable_core_catalogue = input_tables[S_CableCoreCatalogue.DATABASE_NAME.value]
    cable_catalogue_number_master = input_tables[S_CableCatalogueNumber_Master.DATABASE_NAME.value]
    cable_catalogue = input_tables[S_CableCatalogue.DATABASE_NAME.value]
    
    merged_df = cable_core_catalogue.merge(
            cable_catalogue_number_master,
            left_on=[S_CableCoreCatalogue.DATABASE_NAME.value, S_CableCoreCatalogue.CABLE_OBJECT_IDENTIFIER.value],
            right_on=[S_CableCatalogueNumber_Master.DATABASE_NAME.value,
                      S_CableCatalogueNumber_Master.CABLE_OBJECT_IDENTIFIER.value]
            )
    
    merged_df = merged_df.merge(
            cable_catalogue,
            left_on=[S_CableCoreCatalogue.DATABASE_NAME.value, S_CableCoreCatalogue.OBJECT_IDENTIFIER.value],
            right_on=[S_CableCatalogue.DATABASE_NAME.value, S_CableCatalogue.OBJECT_IDENTIFIER.value]
            )
    
    classes = ['Instrumentation', 'Inst(Shared)', 'Elec(Shared)']
    merged_df = merged_df[merged_df[S_CableCatalogue.CLASS.value].isin(
        classes)]
    
    merged_df["RNT"] = merged_df.groupby(
        S_CableCatalogueNumber_Master.CATALOGUENO.value).cumcount() + 1
    
    return merged_df


def _get_final_df(
        cable_core_catalogue):
    return cable_core_catalogue[cable_core_catalogue["RNT"] == 1][
        [S_CableCatalogueNumber_Master.CATALOGUENO.value, S_CableCatalogue.DESCRIPTION.value,
         S_CableCoreCatalogue.GROUP_MARKING.value, S_CableCoreCatalogue.GROUP_MARKING_SEQUENCE.value,
         S_CableCoreCatalogue.CORE_MARKINGS.value, S_CableCoreCatalogue.CORE_MARKINGS_CORE_TYPE.value]
    ]


# Main function
def create_dataframe_gold_c02_instrument_type_catalogue_sql_01_00(
        input_tables):
    database_names = _get_database_names(
        input_tables)
    cable_core_catalogue = _get_cable_core_catalogue(
        input_tables)
    final_df = _get_final_df(
        cable_core_catalogue)
    
    return final_df
