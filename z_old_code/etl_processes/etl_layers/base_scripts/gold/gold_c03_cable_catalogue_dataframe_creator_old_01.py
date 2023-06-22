# from typing import (
#     List)
# import pandas
#
#
# def create_dataframe_gold_c03_cable_catalogue(
#         cable_catalogue_dataframe: pandas.DataFrame,
#         cable_master_dataframe: pandas.DataFrame,
#         database_names_dataframe: pandas.DataFrame) \
#         -> pandas.DataFrame:
#     # identical to list in c03_Cable_Core_Catalogue
#     filter_list = \
#         [
#             'Instrumentation',
#             'Inst(Shared)',
#             'Elec(Shared)'
#         ]
#
#     # Note: added by hand because GPT-4 didn't generate the column filtering
#     columns_to_keep = \
#         [
#             'CatalogueNo',
#             'Manufacturer',
#             'Class',
#             'Description',
#             'GroupType',
#             'NoOfGroups',
#             'Armoured',
#             'OAScr',
#             'GroupScr',
#             'EarthCore',
#             'Voltage',
#             'Size',
#             'Earth_Core_Size',
#             'OD',
#             'Material',
#             'Colour1',
#             'Colour2',
#             'AllowUse',
#             'DrumLength',
#             'LineType',
#             'LineTypeColor',
#             'LineTypeWidth',
#             'LineTypeArrowHead',
#             'Remarks'
#         ]
#
#     filtered_classes = \
#         __filter_classes(
#             cable_catalogue_dataframe,
#             filter_list)
#
#     filtered_database_names = \
#         __filter_database_names(
#             filtered_classes,
#             database_names_dataframe)
#
#     # Note: added by hand because original query: 03_Cable_Catalogue.sql does not keep this column
#     cable_master_dataframe = \
#         cable_master_dataframe.drop('Description', axis=1)
#
#     merged_data = \
#         __merge_dataframes(
#             filtered_database_names,
#             cable_master_dataframe)
#
#     row_number_data = \
#         __calculate_row_number(
#             merged_data)
#
#     cable_catalogue = \
#         __filter_by_row_number(
#             row_number_data,
#             1)
#
#     # Note: added by hand to filter the set of columns
#     cable_catalogue = \
#         cable_catalogue.filter(
#             items=columns_to_keep)
#
#     return \
#         cable_catalogue
#
#
# def __create_immutable_dataframe(
#         dataframe: pandas.DataFrame) \
#         -> pandas.DataFrame:
#     return dataframe.copy()
#
#
# def __filter_classes(
#         dataframe: pandas.DataFrame,
#         classes: List[str]) \
#         -> pandas.DataFrame:
#     return dataframe[dataframe['Class'].isin(
#         classes)]
#
#
# def __filter_database_names(
#         dataframe: pandas.DataFrame,
#         database_names: pandas.DataFrame) \
#         -> pandas.DataFrame:
#     # Note: it was changed by hand: from 'database_name' to 'Database_name'
#     return dataframe[dataframe['Database_name'].isin(
#         database_names['Database_name'])]
#
#
# def __merge_dataframes(
#         cable_catalogue: pandas.DataFrame,
#         cable_master: pandas.DataFrame) \
#         -> pandas.DataFrame:
#     # Note: it was changed by hand: from 'database_name' to 'Database_name'
#     return cable_catalogue.merge(
#         cable_master,
#         how='inner',
#         left_on=['Database_name', 'Object_Identifier'],
#         right_on=['Database_name', 'Cable_Object_Identifier'])
#
#
# def __calculate_row_number(
#         dataframe: pandas.DataFrame) \
#         -> pandas.DataFrame:
#     dataframe['RNT'] = dataframe.groupby(
#         'CatalogueNo').cumcount() + 1
#     return dataframe
#
#
# def __filter_by_row_number(
#         dataframe: pandas.DataFrame,
#         row_number: int) \
#         -> pandas.DataFrame:
#     return \
#         dataframe[dataframe['RNT'] == row_number]
