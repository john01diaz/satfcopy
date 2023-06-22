import pandas


def add_row_number(
        dataframe: pandas.DataFrame,
        partition_column: str,
        order_column: str) -> pandas.DataFrame:
    dataframe = dataframe.sort_values(
        by=[partition_column, order_column])
    dataframe['RNT'] = dataframe.groupby(
        partition_column).cumcount() + 1
    return dataframe


def process_cable_catalogue(
        cable_catalogue: pandas.DataFrame,
        cable_catalogue_master: pandas.DataFrame,
        database_names: pandas.DataFrame) -> pandas.DataFrame:
    classes = ['Instrumentation', 'Inst(Shared)', 'Elec(Shared)']
    
    filtered_cable_catalogue = cable_catalogue[cable_catalogue[S_CableCatalogue.CLASS.value].isin(
        classes)
                                               & cable_catalogue[S_CableCatalogue.DATABASE_NAME.value].isin(
            database_names[DatabaseNames.DATABASE_NAME.value])]
    
    # Filtering cable_catalogue_master to include only necessary columns before merging
    necessary_columns_cable_catalogue_master = [S_CableCatalogueNumber_Master.CATALOGUENO.value,
                                                S_CableCatalogueNumber_Master.CABLE_OBJECT_IDENTIFIER.value,
                                                S_CableCatalogueNumber_Master.DATABASE_NAME.value]
    filtered_cable_catalogue_master = cable_catalogue_master[necessary_columns_cable_catalogue_master]
    
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
    return final_dataframe[selected_columns]
