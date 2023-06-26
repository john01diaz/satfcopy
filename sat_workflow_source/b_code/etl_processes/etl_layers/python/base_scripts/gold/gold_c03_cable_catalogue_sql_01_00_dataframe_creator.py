import pandas as pd
from sat_workflow_source.b_code.etl_schemas.silver_stage.s_cable_catalogue import S_CableCatalogue
from sat_workflow_source.b_code.etl_schemas.silver_stage.s_cable_catalogue_number_master import \
    S_CableCatalogueNumber_Master


def _cte_cable_catalogue(
        input_tables):
    cable_catalogue = input_tables[f"Sigraph_Silver.{S_CableCatalogue.__name__}"]
    cable_catalogue_master = input_tables[f"Sigraph_Silver.{S_CableCatalogueNumber_Master.__name__}"]
    database_names_table = input_tables["VW_Database_names"]
    
    joined_table = pd.merge(
        cable_catalogue,
        cable_catalogue_master,
        left_on=[S_CableCatalogue.DATABASE_NAME.value, S_CableCatalogue.OBJECT_IDENTIFIER.value],
        right_on=[S_CableCatalogueNumber_Master.DATABASE_NAME.value,
                  S_CableCatalogueNumber_Master.CABLE_OBJECT_IDENTIFIER.value],
        suffixes=('', 'right'))
    
    class_criteria = ['Instrumentation', 'Inst(Shared)', 'Elec(Shared)']
    joined_table = joined_table[joined_table[S_CableCatalogue.CLASS.value].isin(
        class_criteria)]
    
    database_names = database_names_table['database_name'].unique().tolist()
    joined_table = joined_table[joined_table[S_CableCatalogue.DATABASE_NAME.value].isin(
        database_names)]
    
    joined_table['RNT'] = joined_table.groupby(
        S_CableCatalogueNumber_Master.CATALOGUENO.value).cumcount() + 1
    
    return joined_table


def _final_select_statement(
        cte_cable_catalogue):
    cte_cable_catalogue = cte_cable_catalogue[cte_cable_catalogue['RNT'] == 1]
    selected_columns = [
        S_CableCatalogueNumber_Master.CATALOGUENO.value,
        S_CableCatalogue.MANUFACTURER.value,
        S_CableCatalogue.CLASS.value,
        S_CableCatalogue.DESCRIPTION.value,
        S_CableCatalogue.GROUPTYPE.value,
        S_CableCatalogue.NOOFGROUPS.value,
        S_CableCatalogue.ARMOURED.value,
        S_CableCatalogue.OASCR.value,
        S_CableCatalogue.GROUPSCR.value,
        S_CableCatalogue.EARTHCORE.value,
        S_CableCatalogue.VOLTAGE.value,
        S_CableCatalogue.SIZE.value,
        S_CableCatalogue.EARTH_CORE_SIZE.value,
        S_CableCatalogue.OD.value,
        S_CableCatalogue.MATERIAL.value,
        S_CableCatalogue.COLOUR1.value,
        S_CableCatalogue.COLOUR2.value,
        S_CableCatalogue.ALLOWUSE.value,
        S_CableCatalogue.DRUMLENGTH.value,
        S_CableCatalogue.LINETYPE.value,
        S_CableCatalogue.LINETYPECOLOR.value,
        S_CableCatalogue.LINETYPEWIDTH.value,
        S_CableCatalogue.LINETYPEARROWHEAD.value,
        S_CableCatalogue.REMARKS.value
        ]
    return cte_cable_catalogue[selected_columns]


def create_dataframe_gold_c03_cable_catalogue_sql_01_00(
        input_tables):
    cte_cable_catalogue = _cte_cable_catalogue(
        input_tables)
    return _final_select_statement(
        cte_cable_catalogue)
