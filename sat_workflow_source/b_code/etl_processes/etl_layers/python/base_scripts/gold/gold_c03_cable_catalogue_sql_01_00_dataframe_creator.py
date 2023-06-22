import pandas

from sat_workflow_source.b_code.etl_schemas.common.database_names import DatabaseNames
from sat_workflow_source.b_code.etl_schemas.gold_stage.cable_catalogue import Cable_Catalogue
from sat_workflow_source.b_code.etl_schemas.silver_stage.s_cable_catalogue import S_CableCatalogue
from sat_workflow_source.b_code.etl_schemas.silver_stage.s_cable_catalogue_number_master import \
    S_CableCatalogueNumber_Master


def add_row_number(
        dataframe: pandas.DataFrame,
        partition_column: str,
        order_column: str) -> pandas.DataFrame:
    dataframe = dataframe.sort_values(
            by=[partition_column, order_column])
    dataframe['RNT'] = dataframe.groupby(
            partition_column).cumcount() + 1
    return dataframe


def create_dataframe_gold_c03_cable_catalogue_sql_01_00(
        input_tables: dict) -> pandas.DataFrame:
    cable_catalogue_dataframe = \
        input_tables['S_CableCatalogue']

    cable_catalogue_number_master_dataframe = \
        input_tables['S_CableCatalogueNumber_Master']

    database_names_dataframe = \
        input_tables['database_names']

    classes = ['Instrumentation', 'Inst(Shared)', 'Elec(Shared)']
    
    filtered_cable_catalogue = cable_catalogue_dataframe[cable_catalogue_dataframe[S_CableCatalogue.CLASS.value].isin(
            classes)
                                                         & cable_catalogue_dataframe[S_CableCatalogue.DATABASE_NAME.value].isin(
            database_names_dataframe[DatabaseNames.DATABASE_NAME.value])]
    
    # Filtering cable_catalogue_master to include only necessary columns before merging
    necessary_columns_cable_catalogue_master = [S_CableCatalogueNumber_Master.CATALOGUENO.value,
                                                S_CableCatalogueNumber_Master.CABLE_OBJECT_IDENTIFIER.value,
                                                S_CableCatalogueNumber_Master.DATABASE_NAME.value]
    filtered_cable_catalogue_master = cable_catalogue_number_master_dataframe[necessary_columns_cable_catalogue_master]
    
    merged_dataframe = pandas.merge(
            filtered_cable_catalogue,
            filtered_cable_catalogue_master,
            how='inner',
            left_on=[S_CableCatalogue.DATABASE_NAME.value, S_CableCatalogue.OBJECT_IDENTIFIER.value],
            right_on=[S_CableCatalogueNumber_Master.DATABASE_NAME.value,
                      S_CableCatalogueNumber_Master.CABLE_OBJECT_IDENTIFIER.value])
    
    merged_dataframe_with_rnt = add_row_number(
            merged_dataframe,
            S_CableCatalogueNumber_Master.CATALOGUENO.value,
            S_CableCatalogueNumber_Master.CABLE_OBJECT_IDENTIFIER.value)
    final_dataframe = merged_dataframe_with_rnt[merged_dataframe_with_rnt['RNT'] == 1]
    
    selected_columns = [
        Cable_Catalogue.CATALOGUENO.value,
        Cable_Catalogue.MANUFACTURER.value,
        Cable_Catalogue.CLASS.value,
        Cable_Catalogue.DESCRIPTION.value,
        Cable_Catalogue.GROUPTYPE.value,
        Cable_Catalogue.NOOFGROUPS.value,
        Cable_Catalogue.ARMOURED.value,
        Cable_Catalogue.OASCR.value,
        Cable_Catalogue.GROUPSCR.value,
        Cable_Catalogue.EARTHCORE.value,
        Cable_Catalogue.VOLTAGE.value,
        Cable_Catalogue.SIZE.value,
        Cable_Catalogue.EARTH_CORE_SIZE.value,
        Cable_Catalogue.OD.value,
        Cable_Catalogue.MATERIAL.value,
        Cable_Catalogue.COLOUR1.value,
        Cable_Catalogue.COLOUR2.value,
        Cable_Catalogue.ALLOWUSE.value,
        Cable_Catalogue.DRUMLENGTH.value,
        Cable_Catalogue.LINETYPE.value,
        Cable_Catalogue.LINETYPECOLOR.value,
        Cable_Catalogue.LINETYPEWIDTH.value,
        Cable_Catalogue.LINETYPEARROWHEAD.value,
        Cable_Catalogue.REMARKS.value
        ]

    # Note: MANUAL STEPS - Drop duplicates
    cable_catalogue_dataframe = \
        final_dataframe[selected_columns]

    cable_catalogue_dataframe_deduplicated = \
        cable_catalogue_dataframe.drop_duplicates()

    output_dataframe = cable_catalogue_dataframe_deduplicated.replace(
            {
                None: 'null'
                })

    return output_dataframe
