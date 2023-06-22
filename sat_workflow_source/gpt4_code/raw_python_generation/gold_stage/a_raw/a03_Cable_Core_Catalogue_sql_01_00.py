def add_dense_rank(
        dataframe: pandas.DataFrame,
        partition_column: str,
        order_columns: list) -> pandas.DataFrame:
    dataframe['combined_order_columns'] = dataframe[order_columns].astype(
        str).agg(
        '-'.join,
        axis=1)
    dataframe['RNT'] = dataframe.groupby(
        partition_column)['combined_order_columns'].rank(
        method='dense',
        ascending=True)
    dataframe.drop(
        columns=['combined_order_columns'],
        inplace=True)
    return dataframe


def process_cable_core_catalogue(
        cable_core_catalogue: pandas.DataFrame,
        cable_catalogue_master: pandas.DataFrame,
        cable_catalogue: pandas.DataFrame,
        database_names: pandas.DataFrame) -> pandas.DataFrame:
    
    classes = ['Instrumentation', 'Inst(Shared)', 'Elec(Shared)']
    
    # Filtering cable_catalogue to include only necessary columns before merging
    necessary_columns_cable_catalogue = [S_CableCatalogue.DATABASE_NAME.value,
                                         S_CableCatalogue.OBJECT_IDENTIFIER.value,
                                         S_CableCatalogue.CLASS.value,
                                         S_CableCatalogue.DESCRIPTION.value]
    filtered_cable_catalogue = cable_catalogue[necessary_columns_cable_catalogue]
    filtered_cable_catalogue = filtered_cable_catalogue[filtered_cable_catalogue[S_CableCatalogue.CLASS.value].isin(
        classes)
                                                        & filtered_cable_catalogue[
                                                            S_CableCatalogue.DATABASE_NAME.value].isin(
            database_names[DatabaseNames.DATABASE_NAME.value])]
    
    # Filtering cable_catalogue_master to include only necessary columns before merging
    necessary_columns_cable_catalogue_master = [S_CableCatalogueNumber_Master.DATABASE_NAME.value,
                                                S_CableCatalogueNumber_Master.CABLE_OBJECT_IDENTIFIER.value,
                                                S_CableCatalogueNumber_Master.CATALOGUENO.value]
    filtered_cable_catalogue_master = cable_catalogue_master[necessary_columns_cable_catalogue_master]
    
    # Filtering cable_core_catalogue to include only necessary columns before merging
    necessary_columns_cable_core_catalogue = [S_CableCoreCatalogue.DATABASE_NAME.value,
                                              S_CableCoreCatalogue.CABLE_OBJECT_IDENTIFIER.value,
                                              S_CableCoreCatalogue.GROUP_MARKING.value,
                                              S_CableCoreCatalogue.GROUP_MARKING_SEQUENCE.value,
                                              S_CableCoreCatalogue.CORE_MARKINGS.value,
                                              S_CableCoreCatalogue.CORE_MARKINGS_CORE_TYPE.value]
    filtered_cable_core_catalogue = cable_core_catalogue[necessary_columns_cable_core_catalogue]
    
    # Merge operation
    merged_dataframe = filtered_cable_core_catalogue.merge(
        filtered_cable_catalogue_master,
        how='inner',
        on=[S_CableCatalogueNumber_Master.DATABASE_NAME.value,
            S_CableCatalogueNumber_Master.CABLE_OBJECT_IDENTIFIER.value]).merge(
        filtered_cable_catalogue,
        how='inner',
        left_on=[S_CableCatalogueNumber_Master.DATABASE_NAME.value,
                 S_CableCatalogueNumber_Master.CABLE_OBJECT_IDENTIFIER.value],
        right_on=[S_CableCatalogue.DATABASE_NAME.value,
                  S_CableCatalogue.OBJECT_IDENTIFIER.value])
    
    # Applying the window function equivalent
    merged_dataframe_with_rnt = add_dense_rank(
        merged_dataframe,
        S_CableCatalogueNumber_Master.CATALOGUENO.value,
        [S_CableCatalogueNumber_Master.DATABASE_NAME.value,
         S_CableCatalogueNumber_Master.CABLE_OBJECT_IDENTIFIER.value])
    
    final_dataframe = merged_dataframe_with_rnt[merged_dataframe_with_rnt['RNT'] == 1.0]
    
    selected_columns = [
        Cable_Core_Catalogue.CATALOGUENO.value,
        Cable_Core_Catalogue.DESCRIPTION.value,
        Cable_Core_Catalogue.GROUP_MARKING.value,
        Cable_Core_Catalogue.GROUP_MARKING_SEQUENCE.value,
        Cable_Core_Catalogue.CORE_MARKINGS.value,
        Cable_Core_Catalogue.CORE_MARKINGS_CORE_TYPE.value
        ]
    return final_dataframe[selected_columns]
