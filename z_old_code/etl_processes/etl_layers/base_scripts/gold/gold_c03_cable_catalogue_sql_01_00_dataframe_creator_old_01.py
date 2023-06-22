# import pandas
#
# CABLE_CLASSES = \
#     [
#         'Instrumentation',
#         'Inst(Shared)',
#         'Elec(Shared)'
#     ]
#
# COLUMN_ORDER = \
#     [
#         'CatalogueNo',
#         'Manufacturer',
#         'Class',
#         'Description',
#         'GroupType',
#         'NoOfGroups',
#         'Armoured',
#         'OAScr',
#         'GroupScr',
#         'EarthCore',
#         'Voltage',
#         'Size',
#         'Earth_Core_Size',
#         'OD',
#         'Material',
#         'Colour1',
#         'Colour2',
#         'AllowUse',
#         'DrumLength',
#         'LineType',
#         'LineTypeColor',
#         'LineTypeWidth',
#         'LineTypeArrowHead',
#         'Remarks'
#     ]
#
#
# def create_dataframe_gold_c03_cable_catalogue_sql_01_00(
#         cable_catalogue_dataframe: pandas.DataFrame,
#         cable_catalogue_number_master_dataframe: pandas.DataFrame,
#         database_names_dataframe: pandas.DataFrame) \
#         -> pandas.DataFrame:
#     cable_catalogue_dataframe = \
#         __filter_data(
#             cable_catalogue_dataframe,
#             'Class',
#             CABLE_CLASSES)
#
#     # Note: renamed from database_name to Database_name
#     cable_catalogue_dataframe = \
#         __filter_data(
#             cable_catalogue_dataframe,
#             'Database_name',
#             database_names_dataframe['Database_name'].tolist())
#
#     # Note: renamed from database_name to Database_name (twice)
#     joined_dataframe = \
#         __join_data_frames(
#             cable_catalogue_dataframe,
#             cable_catalogue_number_master_dataframe,
#             [('Database_name', 'Object_Identifier'), ('Database_name', 'Cable_Object_Identifier')])
#
#     ranked_dataframe = \
#         __rank_data(
#             joined_dataframe,
#             'CatalogueNo',
#             'Cable_Object_Identifier')
#
#     final_dataframe = \
#         ranked_dataframe[ranked_dataframe['RNT'] == 1].copy()
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
# def __join_data_frames(
#         first_dataframe: pandas.DataFrame,
#         second_dataframe: pandas.DataFrame,
#         join_conditions) \
#         -> pandas.DataFrame:
#     joined_dataframe = \
#         pandas.merge(
#             first_dataframe,
#             second_dataframe,
#             left_on=join_conditions[0],
#             right_on=join_conditions[1])
#
#     return \
#         joined_dataframe
#
#
# def __filter_data(
#         dataframe: pandas.DataFrame,
#         column: str,
#         values: list) \
#         -> pandas.DataFrame:
#     filtered_data_frame = \
#         dataframe[dataframe[column].isin(
#             values)]
#
#     return \
#         filtered_data_frame
#
#
# def __rank_data(
#         dataframe: pandas.DataFrame,
#         partition_column,
#         order_column) \
#         -> pandas.DataFrame:
#     dataframe['RNT'] = \
#         dataframe.sort_values(
#             order_column).groupby(
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
