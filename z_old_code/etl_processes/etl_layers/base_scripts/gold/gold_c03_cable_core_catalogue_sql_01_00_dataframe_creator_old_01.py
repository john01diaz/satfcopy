# import pandas
#
# CABLE_CLASSES = \
#     [
#         'Instrumentation',
#         'Inst(Shared)',
#         'Elec(Shared)'
#     ]
#
# RANK_NUMBER = \
#     1
#
# COLUMN_ORDER = \
#     [
#         'CatalogueNo',
#         'Description',
#         'Group_Marking',
#         'Group_Marking_Sequence',
#         'Core_Markings',
#         'Core_Markings_Core_Type'
#     ]
#
#
# def create_dataframe_gold_c03_cable_core_catalogue_sql_01_00(
#         s_cable_core_catalogue_dataframe: pandas.DataFrame,
#         s_cable_catalogue_number_master_dataframe: pandas.DataFrame,
#         s_cable_catalogue_dataframe: pandas.DataFrame,
#         vw_database_names_dataframe: pandas.DataFrame) \
#         -> pandas.DataFrame:
#     # Note: renamed from database_name to Database_name
#     joined_data = \
#         __join_dataframes(
#             s_cable_core_catalogue_dataframe,
#             s_cable_catalogue_number_master_dataframe,
#             [('database_name', 'Cable_Object_Identifier'),
#              ('Database_name', 'Cable_Object_Identifier')])
#
#     # Note: renamed from database_name to Database_name
#     joined_data = \
#         __join_dataframes(
#             joined_data,
#             s_cable_catalogue_dataframe,
#             [('database_name', 'Cable_Object_Identifier'),
#              ('Database_name', 'Object_Identifier')])
#
#     joined_data = \
#         __filter_data(
#             joined_data,
#             'Class',
#             CABLE_CLASSES)
#
#     joined_data = \
#         __filter_data(
#             joined_data,
#             'database_name',
#             vw_database_names_dataframe['Database_name'].tolist())
#
#     ranked_dataframe = \
#         __rank_data(
#             joined_data,
#             'CatalogueNo',
#             ['database_name', 'Cable_Object_Identifier'])
#
#     final_dataframe = \
#         ranked_dataframe[ranked_dataframe['RNT'] == RANK_NUMBER].copy()
#
#     # Note: added by hand because in the original query does not get the S_CableCatalogueNumber_Master.Description
#     #  but GPT-4 prompt is including both
#     final_dataframe.rename(
#         columns={'Description_x': 'Description'},
#         inplace=True)
#
#     final_dataframe = \
#         __select_columns(
#             final_dataframe,
#             COLUMN_ORDER)
#
#     return \
#         final_dataframe
#
#
# def __join_dataframes(
#         first_dataframe: pandas.DataFrame,
#         second_dataframe: pandas.DataFrame,
#         join_conditions: list):
#     joined_df = \
#         pandas.merge(
#             first_dataframe,
#             second_dataframe,
#             left_on=join_conditions[0],
#             right_on=join_conditions[1])
#
#     return \
#         joined_df
#
#
# def __filter_data(
#         dataframe: pandas.DataFrame,
#         column: str,
#         values: list) \
#         -> pandas.DataFrame:
#     filtered_dataframe = \
#         dataframe[dataframe[column].isin(
#             values)]
#
#     return \
#         filtered_dataframe
#
#
# def __rank_data(
#         dataframe: pandas.DataFrame,
#         partition_column: str,
#         order_columns: list) \
#         -> pandas.DataFrame:
#     dataframe['RNT'] = \
#         dataframe.sort_values(
#             order_columns).groupby(
#             partition_column).cumcount() + 1
#
#     return \
#         dataframe
#
#
# def __select_columns(
#         dataframe: pandas.DataFrame,
#         columns: list) \
#         -> pandas.DataFrame:
#     selected_data = \
#         dataframe[columns]
#
#     return \
#         selected_data
