# from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.common.pandas_sql import PandasSQL
from sat_workflow_source.b_code.etl_schemas.common.database_names import DatabaseNames
from sat_workflow_source.b_code.etl_schemas.gold_stage.cable_core_catalogue import Cable_Core_Catalogue
from sat_workflow_source.b_code.etl_schemas.silver_stage.s_cable_catalogue import S_CableCatalogue
from sat_workflow_source.b_code.etl_schemas.silver_stage.s_cable_catalogue_number_master import \
    S_CableCatalogueNumber_Master
from sat_workflow_source.b_code.etl_schemas.silver_stage.s_cable_core_catalogue import S_CableCoreCatalogue
import pandas

# def _get_database_names(
#         input_tables):
#     return input_tables[DatabaseNames.DATABASE_NAME.value]


# def _get_cable_core_catalogue(
#         input_tables):
#     cable_core_catalogue = input_tables[f"Sigraph_Silver.{S_CableCoreCatalogue.__name__}"]
#     cable_catalogue_number_master = input_tables[f"Sigraph_Silver.{S_CableCatalogueNumber_Master.__name__}"]
#     cable_catalogue = input_tables[f"Sigraph_Silver.{S_CableCatalogue.__name__}"]

#     merged_df = cable_core_catalogue.merge(
#             cable_catalogue_number_master,
#             left_on=[S_CableCoreCatalogue.DATABASE_NAME.value, S_CableCoreCatalogue.CABLE_OBJECT_IDENTIFIER.value],
#             right_on=[S_CableCatalogueNumber_Master.DATABASE_NAME.value,
#                       S_CableCatalogueNumber_Master.CABLE_OBJECT_IDENTIFIER.value],
#             suffixes=('', 'right')
#             )

#     # merged_df = merged_df.merge(
#     #         cable_catalogue,
#     #         left_on=[S_CableCoreCatalogue.DATABASE_NAME.value, S_CableCoreCatalogue.OBJECT_IDENTIFIER.value],
#     #         right_on=[S_CableCatalogue.DATABASE_NAME.value, S_CableCatalogue.OBJECT_IDENTIFIER.value],
#     #         suffixes=('', 'right')
#     #         )

#     classes = ['Instrumentation', 'Inst(Shared)', 'Elec(Shared)']
#     merged_df = merged_df[merged_df[S_CableCatalogue.CLASS.value].isin(
#         classes)]

#     merged_df["RNT"] = merged_df.groupby(
#         S_CableCatalogueNumber_Master.CATALOGUENO.value).cumcount() + 1

#     return merged_df


# def _get_final_df(
#         cable_core_catalogue):
#     return cable_core_catalogue[cable_core_catalogue["RNT"] == 1][
#         [S_CableCatalogueNumber_Master.CATALOGUENO.value, S_CableCatalogue.DESCRIPTION.value,
#          S_CableCoreCatalogue.GROUP_MARKING.value, S_CableCoreCatalogue.GROUP_MARKING_SEQUENCE.value,
#          S_CableCoreCatalogue.CORE_MARKINGS.value, S_CableCoreCatalogue.CORE_MARKINGS_CORE_TYPE.value]
#     ]


# # Main function
# def create_dataframe_gold_c03_cable_core_catalogue(
#         input_tables):
#     # database_names = _get_database_names(
#     #     input_tables)
#     cable_core_catalogue = _get_cable_core_catalogue(
#         input_tables)
#     final_df = _get_final_df(
#         cable_core_catalogue)

#     return final_df

# SELECTED_CLASSES = ['Instrumentation', 'Inst(Shared)', 'Elec(Shared)']

# def create_dataframe_gold_c03_cable_core_catalogue(input_tables: dict):
#     # Unpacking dataframes from the input dictionary
#     cable_core_catalogue_df = input_tables[f"Sigraph_Silver.{S_CableCoreCatalogue.__name__}"]
#     cable_catalogue_number_master_df = input_tables[f"Sigraph_Silver.{S_CableCatalogueNumber_Master.__name__}"]
#     cable_catalogue_df = input_tables[f"Sigraph_Silver.{S_CableCatalogue.__name__}"]
#     database_names_df = input_tables['VW_Database_names']

#     # Sub-script 1
#     dataframe_one = __join_cable_core_catalogue_and_master(cable_core_catalogue_df, cable_catalogue_number_master_df)
#     # Sub-script 2
#     dataframe_two = __join_dataframes_one_and_cable_catalogue(dataframe_one, cable_catalogue_df)
#     # Sub-script 3
#     dataframe_three = __filter_dataframe_by_class_and_database_name(dataframe_two, database_names_df)
#     # Sub-script 4
#     dataframe_four = __calculate_dense_rank(dataframe_three)
#     # Sub-script 5
#     dataframe_final = __filter_dataframe_by_rnt(dataframe_four)

#     return dataframe_final

# def __join_cable_core_catalogue_and_master(cable_core_catalogue_df, cable_catalogue_number_master_df):
#     dataframe_one = pandas.merge(cable_core_catalogue_df, cable_catalogue_number_master_df,
#                              left_on=[S_CableCoreCatalogue.DATABASE_NAME.value, S_CableCoreCatalogue.CABLE_OBJECT_IDENTIFIER.value],
#                              right_on=[S_CableCatalogueNumber_Master.DATABASE_NAME.value, S_CableCatalogueNumber_Master.CABLE_OBJECT_IDENTIFIER.value], suffixes=('','right'))
#     return dataframe_one

# def __join_dataframes_one_and_cable_catalogue(dataframe_one, cable_catalogue_df):
#     dataframe_two = pandas.merge(dataframe_one, cable_catalogue_df,
#                              left_on=[S_CableCatalogueNumber_Master.DATABASE_NAME.value, S_CableCatalogueNumber_Master.CABLE_OBJECT_IDENTIFIER.value],
#                              right_on=[S_CableCatalogue.DATABASE_NAME.value, S_CableCatalogue.OBJECT_IDENTIFIER.value], suffixes=('','right')),
#     return dataframe_two

# def __filter_dataframe_by_class_and_database_name(dataframe_two, database_names_df):
#     class_condition = dataframe_two[S_CableCatalogue.CLASS.value].isin(SELECTED_CLASSES)
#     database_name_condition = dataframe_two[S_CableCatalogue.DATABASE_NAME.value].isin(database_names_df[DatabaseNames.DATABASE_NAME.value])

#     dataframe_three = dataframe_two[class_condition & database_name_condition]
#     return dataframe_three

# def __calculate_dense_rank(dataframe_three):
#     dataframe_three["RNT"] = dataframe_three.groupby([Cable_Core_Catalogue.CATALOGUENO.value, S_CableCatalogue.DATABASE_NAME.value, S_CableCatalogue.OBJECT_IDENTIFIER.value])[Cable_Core_Catalogue.CATALOGUENO.value].rank(method='dense')
#     return dataframe_three

# def __filter_dataframe_by_rnt(dataframe_four):
#     dataframe_final = dataframe_four[dataframe_four["RNT"] == 1][[
#         Cable_Core_Catalogue.CATALOGUENO.value,
#         Cable_Core_Catalogue.DESCRIPTION.value,
#         Cable_Core_Catalogue.GROUP_MARKING.value,
#         Cable_Core_Catalogue.GROUP_MARKING_SEQUENCE.value,
#         Cable_Core_Catalogue.CORE_MARKINGS.value,
#         Cable_Core_Catalogue.CORE_MARKINGS_CORE_TYPE.value
#     ]]
#     return dataframe_final

# def create_dataframe_gold_c03_cable_core_catalogue(input_tables: dict):

#     cable_core_catalogue_df = input_tables[f"Sigraph_Silver.{S_CableCoreCatalogue.__name__}"]
#     cable_catalogue_number_master_df = input_tables[f"Sigraph_Silver.{S_CableCatalogueNumber_Master.__name__}"]
#     cable_catalogue_df = input_tables[f"Sigraph_Silver.{S_CableCatalogue.__name__}"]
#     database_names_df = input_tables['VW_Database_names']

#    # Sub-script 1
#     pandas_sql = PandasSQL(cable_core_catalogue_df)
#     C = (
#         pandas_sql.inner_join(cable_catalogue_number_master_df, on=[S_CableCoreCatalogue.DATABASE_NAME.value, S_CableCoreCatalogue.CABLE_OBJECT_IDENTIFIER.value])
#         .get_result()
#     )

#     # Sub-script 2
#     pandas_sql = PandasSQL(C)
#     C = (
#         pandas_sql.inner_join(cable_catalogue_df, on=[S_CableCatalogue.DATABASE_NAME.value, S_CableCatalogue.OBJECT_IDENTIFIER.value])
#         .get_result()
#     )

#     # Sub-script 3
#     database_names = PandasSQL(database_names_df).get_result()[DatabaseNames.DATABASE_NAME.value].unique()
#     C = PandasSQL(C).where(lambda df: df[S_CableCatalogue.CLASS.value].isin(['Instrumentation', 'Inst(Shared)', 'Elec(Shared)']) & df[S_CableCatalogue.DATABASE_NAME.value].isin(database_names)).get_result()

#     # Sub-script 4
#     C['RNT'] = ''
#     pandas_sql = PandasSQL(C)
#     C = (
#         pandas_sql.order_by([S_CableCatalogueNumber_Master.CATALOGUENO.value, S_CableCatalogue.DATABASE_NAME.value, S_CableCatalogue.OBJECT_IDENTIFIER.value])
#         .rank('RNT', method='min', ascending=True)
#         .get_result()
#     )

#     # Final script
#     C = (
#         PandasSQL(C)
#         .select([Cable_Core_Catalogue.CATALOGUENO.value, Cable_Core_Catalogue.DESCRIPTION.value, Cable_Core_Catalogue.GROUP_MARKING.value,
#                  Cable_Core_Catalogue.GROUP_MARKING_SEQUENCE.value, Cable_Core_Catalogue.CORE_MARKINGS.value, Cable_Core_Catalogue.CORE_MARKINGS_CORE_TYPE.value])
#         .where(lambda df: df['RNT'] == 1)
#         .get_result()
#     )

#     return C


# Constants
DATABASE_NAMES_TABLE = "VW_Database_names"
REQUIRED_CLASSES = ['Instrumentation', 'Inst(Shared)', 'Elec(Shared)']


# Private Functions
def __merge_dataframes_on_database_and_cable_id(dataframe_one, dataframe_two):
    merged_dataframe = pandas.merge(
        dataframe_one,
        dataframe_two,
        left_on=[S_CableCoreCatalogue.DATABASE_NAME.value, S_CableCoreCatalogue.CABLE_OBJECT_IDENTIFIER.value],
        right_on=[S_CableCatalogueNumber_Master.DATABASE_NAME.value,
                  S_CableCatalogueNumber_Master.CABLE_OBJECT_IDENTIFIER.value],
        how='inner',
        suffixes=('', 'right')
    )

    return merged_dataframe


def __merge_with_cable_catalogue(merged_dataframe, cable_catalogue_dataframe):
    merged_with_cable_catalogue_dataframe = pandas.merge(
        merged_dataframe,
        cable_catalogue_dataframe,
        left_on=[S_CableCoreCatalogue.DATABASE_NAME.value, S_CableCoreCatalogue.CABLE_OBJECT_IDENTIFIER.value],
        right_on=[S_CableCatalogue.DATABASE_NAME.value, S_CableCatalogue.OBJECT_IDENTIFIER.value],
        how='inner',
        suffixes=('', 'right')
    )
    return merged_with_cable_catalogue_dataframe


def __filter_on_class_and_database_names(merged_dataframe, database_names_dataframe):
    filtered_dataframe = merged_dataframe[
        merged_dataframe[S_CableCatalogue.CLASS.value].isin(REQUIRED_CLASSES) &
        merged_dataframe[S_CableCoreCatalogue.DATABASE_NAME.value].isin(
            database_names_dataframe[DatabaseNames.DATABASE_NAME.value])
        ]
    return filtered_dataframe


def __add_dense_rank_column(filtered_dataframe):
    filtered_dataframe['RNT'] = filtered_dataframe.groupby(Cable_Core_Catalogue.CATALOGUENO.value)[
        S_CableCoreCatalogue.DATABASE_NAME.value].rank(method="dense")
    return filtered_dataframe


def __filter_on_rnt(filtered_dataframe):
    final_dataframe = filtered_dataframe[filtered_dataframe['RNT'] == 1]
    return final_dataframe


def find_desc_mismatch(df1, df2):
    df1 = df1.rename(columns={"description": "description_1"})
    df2 = df2.rename(columns={"description": "description_2"})
    df1['description_1'] = df1['description_1'].str.strip()
    df2['description_2'] = df2['description_2'].str.strip()

    df = pandas.merge(df1, df2, left_on=[S_CableCoreCatalogue.DATABASE_NAME.value,
                                         S_CableCoreCatalogue.CABLE_OBJECT_IDENTIFIER.value],
                      right_on=[S_CableCatalogue.DATABASE_NAME.value, S_CableCatalogue.OBJECT_IDENTIFIER.value],
                      how='outer')  # Using outer join to keep all records

    df_mismatch = df[df['description_1'] != df['description_2']]
    df_mismatch['cable_object_identifier'].drop_duplicates()
    df['description'] = df['description_1'].where(df['description_1'] == df['description_2'], df['description_2'])
    df = df.drop(['description_1', 'description_2'], axis=1)
    return df


def create_dataframe_gold_c03_cable_core_catalogue(input_tables):
    cable_core_catalogue_df = input_tables[f"Sigraph_Silver.{S_CableCoreCatalogue.__name__}"]
    cable_catalogue_number_master_df = input_tables[f"Sigraph_Silver.{S_CableCatalogueNumber_Master.__name__}"]
    cable_catalogue_df = input_tables[f"Sigraph_Silver.{S_CableCatalogue.__name__}"]
    database_names_df = input_tables['VW_Database_names']

    merged_dataframe = __merge_dataframes_on_database_and_cable_id(cable_core_catalogue_df,
                                                                   cable_catalogue_number_master_df)
    cleaned_merged_dataframe = find_desc_mismatch(merged_dataframe, cable_catalogue_df)
    merged_with_cable_catalogue_dataframe = __merge_with_cable_catalogue(cleaned_merged_dataframe, cable_catalogue_df)

    filtered_dataframe = __filter_on_class_and_database_names(merged_with_cable_catalogue_dataframe, database_names_df)
    filtered_dataframe_with_rank = __add_dense_rank_column(filtered_dataframe)
    final_dataframe = __filter_on_rnt(filtered_dataframe_with_rank)

    final_dataframe = final_dataframe[[
        Cable_Core_Catalogue.CATALOGUENO.value,
        Cable_Core_Catalogue.DESCRIPTION.value,
        Cable_Core_Catalogue.GROUP_MARKING.value,
        Cable_Core_Catalogue.GROUP_MARKING_SEQUENCE.value,
        Cable_Core_Catalogue.CORE_MARKINGS.value,
        Cable_Core_Catalogue.CORE_MARKINGS_CORE_TYPE.value
    ]]

    # final_dataframe[Cable_Core_Catalogue.DESCRIPTION.value] = final_dataframe[Cable_Core_Catalogue.DESCRIPTION.value].str.strip()
    final_dataframe[Cable_Core_Catalogue.DESCRIPTION.value] = final_dataframe[
        Cable_Core_Catalogue.DESCRIPTION.value].str.replace('  ', ' ')
    final_dataframe[Cable_Core_Catalogue.DESCRIPTION.value] = final_dataframe[
                                                                  Cable_Core_Catalogue.DESCRIPTION.value] + " "
    # final_dataframe = final_dataframe[[Cable_Core_Catalogue.CATALOGUENO.value, Cable_Core_Catalogue.DESCRIPTION.value, Cable_Core_Catalogue.GROUP_MARKING.value]].drop_duplicates()
    final_dataframe = final_dataframe.drop_duplicates()
    final_dataframe = final_dataframe.drop_duplicates(
        subset=[Cable_Core_Catalogue.CATALOGUENO.value, Cable_Core_Catalogue.DESCRIPTION.value,
                Cable_Core_Catalogue.CORE_MARKINGS.value])

    return final_dataframe
# E-YYMY 10X2X0,8